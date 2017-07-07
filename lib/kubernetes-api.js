'use strict';

const _ = require('lodash');
const async = require('async');
const flatten = require('flat');
const os = require('os');
const request = require('request');
const EventEmitter = require('events');
const encode = require('hashcode').hashCode().value;

const Translator = require('@containership/containership.k8s.translator');
const ApiImplementation  = require('@containership/containership.abstraction.api');

const util = require('./util');
const CSUtil = require('containership.utils');

const DEFAULT_K8S_API_URL = 'http://localhost:8080';
const DEFAULT_ETCD_API_URL = 'http://localhost:2379';
class KubernetesApi extends ApiImplementation {

    constructor(k8sApiIP, k8sApiPort) {
        super();

        this.k8sApiIP = k8sApiIP;

        this.k8sApiUrl = k8sApiIP && k8sApiPort ?
            `http://${k8sApiIP}:${k8sApiPort}` :
            DEFAULT_K8S_API_URL;

        this.etcdApiUrl = k8sApiIP ?
            `http://${k8sApiIP}:2379` :
            DEFAULT_ETCD_API_URL;

        this.defaultErrorHandler = (err) => {

            // eslint-disable-next-line no-console
            console.log(`There was an error in the K8S Api Bridge:`);
            // eslint-disable-next-line no-console
            console.error(err);
        };

        this.ifSuccessfulResponse =
            CSUtil.ifAcceptableResponseFn(

                // On Error.
                (err, res, options) => {

                    const errHandler   = _.get(options, ['errorHandler']);
                    const failedMethod = _.get(options, ['method'], 'unknown-method');

                    // eslint-disable-next-line no-console
                    console.log(`Failed making call to K8S API in ${failedMethod} // err: ${err}, res: ${res.statusCode} - ${JSON.stringify(res.body)}.`);

                    if(errHandler) {
                        errHandler(err);
                    }

                },

                //Check acceptance
                (res) => Math.floor(res.statusCode / 200) === 1);

    }

    static k8sServiceDescriptionFor(appDesc, ports, discoveryPorts) {
        const appName = _.get(appDesc, ['metadata', 'name']);

        return{
            kind: 'Service',
            metadata: {
                name: `ser${_.join(_.take(String(encode(appName)), 19), '')}` // Limit to 24 chars
            },
            spec: {
                type: 'NodePort',
                ports: _.map(ports, (port, idx) => {
                    return _.merge({
                        port: port.containerPort
                    }, discoveryPorts[idx] ?
                        {nodePort: discoveryPorts[idx]} : {});
                }),
                selector: {
                    app: _.get(appDesc, ['metadata', 'name'])
                }
            }
        };
    }

    //private, k8s services are analogous to cs service-discovery.
    createService(k8sAppDesc, k8sPorts, discoveryPorts, cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        const service_body = KubernetesApi.k8sServiceDescriptionFor(k8sAppDesc, k8sPorts, discoveryPorts);
        const app_name = _.get(k8sAppDesc, ['metadata', 'name']);

        const req = {
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/services`,
            method: 'POST',
            json: true,
            body: service_body
        };

        return request(req, (err, res) => {
            if(err || res.statusCode !== 201) {
                return errorHandler(`Could not create service: ${service_body.metadata.name} for application: ${app_name}`);
            }

            return handler();
        });
    }

    deleteService(app_name, cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        const service_name = `ser${_.join(_.take(String(encode(app_name)), 19), '')}`;

        const req = {
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/services/${service_name}`,
            method: 'DELETE'
        };

        return request(req, (err, res) => {
            if(err || res.statusCode !== 200) {
                return errorHandler(`Failed to delete service: ${service_name}`);
            }

            return handler();
        });
    }


    enforceAllConstraints() {
        this.getApplications((apps) => {
            _.forEach(apps, (app) => {
                this.updateApplication(app.id, {}, {
                    errorHandler: (err) => {
                        // eslint-disable-next-line no-console
                        console.log(`Error updating application when enforcing constraints for ${app.id}: ${err}`);
                    },
                    handler: () => {
                        // eslint-disable-next-line no-console
                        console.log(`Enforced constraints for ${app.id}`);
                    }
                });
            });
        });
    }

    getApplications(cb) {
        const { handler, errorHandler } = this.sanitizeCallback(cb);

        return async.parallel({
            replicationControllers: (cb) => {
                return request(`${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers`,
                    (err, res, body) => {
                        if(err || res.statusCode !== 200) {
                            return cb('Failed to retrieve replication controllers inside getApplications');
                        }

                        return cb(null, util.safeParse(body));
                    });
            },
            services: (cb) => {
                return request(`${this.k8sApiUrl}/api/v1/namespaces/default/services`,
                    (err, res, body) => {
                        if(err || res.statusCode !== 200) {
                            return cb('Failed to retrieve the services inside getApplications');
                        }

                        return cb(null, util.safeParse(body));
                    });
            },
            hosts: (cb) => {
                return this.getHosts((err, hosts) => {
                    return cb(err, hosts);
                });
            },
            containers: (cb) => {
                return this.getContainers((err, containers) => {
                    return cb(err, containers);
                });
            }
        }, (err, responses) => {
            if(err) {
                return errorHandler(err);
            }

            const replicationControllers = responses.replicationControllers;
            const services = responses.services;

            const k8sItems = _.get(replicationControllers, 'items', []);
            const k8sServices = _.get(services, 'items', []);

            const allContainers = responses.containers;

            const containersByApp = _.reduce(allContainers, (result, container) => {
                const appName = _.get(container, 'name');
                const containers = _.get(result, appName, []);
                return _.set(result, appName, _.concat(containers, container));
            }, {});

            const csItems = _.map(k8sItems, _.flow(
                Translator.csApplicationFromK8SReplicationController,
                (csApp) => {
                    //Augment the app with the discovery port.
                    const k8sServiceForApp = _.first(_.filter(k8sServices, (s) =>
                        _.get(s, ['spec', 'selector', 'app']) === csApp.id));

                    const discoveryPort = _.get(k8sServiceForApp, ['spec', 'ports', 0, 'nodePort']);

                    return discoveryPort ?
                        _.merge(csApp, {'discovery_port': discoveryPort}) :
                        csApp;

                },
                (csApp) => {
                    // filter out containership specific environment variables
                    csApp.env_vars =_.omitBy(csApp.env_vars, (value, key) => {
                        // check if port was manually defined by the end user
                        if(key === 'PORT' && value === csApp.container_port) {
                            return true;
                        // strip all keys with 'CS_' prefix
                        } else if(key.indexOf('CS_') === 0) {
                            return true;
                        // all other keys are valid
                        } else {
                            return false;
                        }
                    });

                    return csApp;
                },
                (csApp) => {
                    return _.set(csApp, 'containers',
                        _.get(containersByApp, csApp.id) || []);
                }));

            const csApps =
                _.merge.apply(null, _.map(csItems, (item) => {
                    return _.set({}, item.id, _.defaults(item, {
                        env_vars: {},
                        tags: {},
                        volumes: []
                    }));
                }));

            return handler(csApps);
        });
    }

    getApplication(application, cb) {
        const { handler, errorHandler } = this.sanitizeCallback(cb);

        return this.getApplications((err, apps) => {
            if(err) {
                return errorHandler(err);
            }

            const app = _.first(_.filter(apps, (v, k) => k == application));

            if(app) {
                return handler(app);
            } else {
                return errorHandler(`The requested application does not exist: ${application}`);
            }
        });
    }

    getContainers(cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        return request(`${this.k8sApiUrl}/api/v1/pods`,
            (err, res, body) => {
                if(err || res.statusCode !== 200) {
                    return errorHandler('Failed to retrive k8s pods inside getContainers call');
                }

                const pods = util.safeParse(body).items;

                const containers = _.map(pods, pod => {
                    const nodeName = _.get(pod, 'spec.nodeName');
                    const k8sContainer = _.get(pod, 'spec');

                    const status = Translator.csStatusFromK8SStatus(_.get(pod, 'status'));

                    const start_time = Date.parse(_.get(pod, 'status.startTime'));
                    const containerID = _.get(pod, 'metadata.uid');
                    const app_name = _.get(pod, 'metadata.labels.app');
                    const name = _.get(pod, 'status.containerStatuses[0].name');
                    const image = _.get(pod, 'status.containerStatuses[0].image');

                    const csContainer = _.merge(
                        Translator.csApplicationFromK8SPodSpec(k8sContainer),
                        {
                            status: status,
                            container_id: containerID,
                            id: containerID,
                            name: name,
                            image: image,
                            host: nodeName,
                            start_time: start_time,
                            application: app_name
                        }
                    );

                    return csContainer;
                });

                return handler(containers);
            });
    }

    createContainers(appId, containerConfig, cb) {
        const { errorHandler } = this.sanitizeCallback(cb);

        return this.getApplications((err, apps) => {
            if(err) {
                return errorHandler(err);
            }

            const app = _.first(_.filter(apps, (v, k) => k == appId));

            if(!app) {
                return errorHandler(Error(`Couldn't find application with id ${appId} when in KubernetesApi::createContainer!`));
            }

            const containerCount = _.size(app.containers);
            const newCount = containerCount + parseInt(containerConfig.count);

            this.updateApplication(appId, _.merge(containerConfig, {
                count: newCount
            }), cb);
        });
    }

    deleteContainer(appId, containerId, cb) {
        const { errorHandler } = this.sanitizeCallback(cb);

        return this.getApplications((err, apps) => {
            if(err) {
                return errorHandler(err);
            }

            const app = _.first(_.filter(apps, (v, k) => k == appId));

            if(!app) {
                return errorHandler(Error(`Couldn't find application with id ${appId} when in KubernetesApi::createContainer!`));
            }

            const containerCount = _.size(app.containers);
            const newCount = containerCount - 1;

            return this.updateApplication(appId, { count: newCount }, cb);
        });
    }

    _redeployContainers(appId, cb) {
        const { handler, errorHandler } = this.sanitizeCallback(cb);

        const getRunningContainerCount = (runningContainerCb) => {
            this.getApplication(appId, (err, app) => {
                if(err) {
                    return runningContainerCb(err);
                }

                return runningContainerCb(null, _.size(app.containers));
            });
        };

        async.waterfall([
            // get number of running containers
            getRunningContainerCount,

            // scale application to 0 containers
            (desired_containers, cb) => {
                this.scaleDownContainers(appId, 0, {
                    errorHandler: cb,
                    handler: () => {
                        return cb(null, desired_containers);
                    }
                });
            },

            // wait until number of containers has reached 0
            (desired_containers, cb) => {
                async.retry({ times: 5, interval: 4000 }, (retryCb) => {
                    getRunningContainerCount((err, num_containers) => {
                        if(err) {
                            return retryCb(err);
                        } else if(num_containers !== 0) {
                            return retryCb(new Error(`${num_containers} containers still running ...`));
                        } else {
                            return retryCb(null, desired_containers);
                        }
                    });
                }, cb);
            },

            (desired_containers, cb) => {
                this.scaleUpContainers(appId, desired_containers, cb);
            }
        ], (err, response) => {
            if(err) {
                return errorHandler(err);
            } else {
                return handler(response);
            }
        });
    }

    _applyConstraintsToApplication(app, cb) {
        const csAppDesc = _.cloneDeep(app);

        this.getHosts((err, hosts) => {
            if(err) {
                return cb(err);
            }

            const constraints = _.get(csAppDesc, ['tags', 'constraints'], {});

            const perHost = parseInt(_.get(constraints, ['per_host'], 0));
            const min = parseInt(_.get(constraints, ['min'], 0));
            const max = parseInt(_.get(constraints, ['max'], Number.MAX_SAFE_INTEGER));

            const running = csAppDesc.count !== undefined ? csAppDesc.count : _.size(csAppDesc.containers);

            const followers = _.filter(_.values(hosts), (h) => h.mode === 'follower');
            const followerCount = _.size(followers);
            const neededReplicas = perHost > 0 ? followerCount * perHost : running;

            const desiredCount =
                neededReplicas > max ? max :
                neededReplicas < min ? min :
                neededReplicas;

            if(desiredCount !== running) {
                csAppDesc.count = desiredCount;
            }

            return cb(null, csAppDesc);
        });
    }

    updateApplication(appId, csAppDesc, cb) {
        const { handler, errorHandler } = this.sanitizeCallback(cb);

        this.getApplication(appId, (err, existingApp) => {
            if(err) {
                return errorHandler(err);
            }

            let properties = _.keys(flatten(csAppDesc));

            const replicationControllerNeedsUpdated = !_.isEmpty(properties);

            _.remove(properties, (property) => {
                return property.indexOf('tags.constraints') === 0 || property === 'count';
            });

            const containersNeedRedeployed = !_.isEmpty(properties);

            const updateReplicationController = (cb) => {
                _.merge(existingApp, csAppDesc);

                this._applyConstraintsToApplication(existingApp, (err, app) => {
                    if(err) {
                        return cb(err);
                    }

                    if(!replicationControllerNeedsUpdated && _.isEqual(existingApp, app)) {
                        return cb();
                    } else {
                        existingApp = app;
                    }

                    const k8sDesc = Translator.csApplicationToK8SReplicationController(existingApp);

                    return request({
                        uri: `${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers/${appId}`,
                        method: 'PATCH',
                        body: k8sDesc,
                        json: true,
                        headers: {
                            'Content-Type': 'application/merge-patch+json'
                        }
                    }, (err, res, body) => {
                        if(err || res.statusCode !== 200) {
                            return cb(err || body);
                        } else {
                            return cb(null, body);
                        }
                    });
                });
            };

            updateReplicationController((err, response) => {
                if(err) {
                    return errorHandler(err);
                }

                if(containersNeedRedeployed) {
                    return this._redeployContainers(appId, cb);
                } else {
                    return handler();
                }
            });
        });
    }

    createApplication(csAppDesc, cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        // only use provided host port if it is in `host` network mode
        const hostPort = (csAppDesc.network_mode && csAppDesc.network_mode === 'host') ? csAppDesc.host_port : null;
        csAppDesc.container_port = hostPort ? hostPort : (csAppDesc.container_port ? csAppDesc.container_port : _.random(11024, 22047));

        // temporary shim for managed volumes in containership. builds a predefined
        // host volume based on path and a uuid. note this will not be unique if
        // more than one container is running on the same host currently
        csAppDesc.volumes = _.map(csAppDesc.volumes || [], (vol) => {
            if (_.isUndefined(vol.host)) {
                vol.host = `/opt/containership/codexd/${csAppDesc.id}`;
            }

            return vol;
        });

        // set the number of containers to 0 for the application unless explicitly set in request
        csAppDesc.count = csAppDesc.containers ? csAppDesc.containers.length : 0;

        const k8sAppDesc = Translator.csApplicationToK8SReplicationController(csAppDesc);

        return request({
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers`,
            method: 'POST',
            body: k8sAppDesc,
            json: true
        }, (err, res, body) => {
            if(err || res.statusCode !== 201) {
                return errorHandler(`Could not create replication controller for ${csAppDesc.id}`);
            }

            const ports = _.get(k8sAppDesc, ['spec', 'template', 'spec', 'containers', 0, 'ports'], []);

            if(_.size(ports) === 0) {
                return handler(body);
            }

            return this.createService(k8sAppDesc, ports, [], (err) => {
                if(err) {
                    return errorHandler(err);
                }

                return handler(body);
            });
        });
    }

    createApplications(csAppsDesc, options, cb) {
        if (typeof options === 'function') {
            cb = options;
            options = {};
        }

        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        return async.waterfall([
            // get applications
            (get_cb) => {
                return this.getApplications((err, existing_apps) => {
                    return get_cb(err, existing_apps);
                });
            },
            (existing_apps, remove_cb) => {
                if (options.remove_existing !== true) {
                    return setImmediate(() => {
                        return remove_cb(null, existing_apps);
                    });
                }

                return async.eachLimit(existing_apps, 10, (app, cb) => {
                    return this.deleteApplication(app.id, (err) => {
                        if (err) {
                            return cb(err);
                        }

                        return cb();
                    });
                }, (err) => {
                    return remove_cb(err, []);
                });
            },
            (existing_apps, add_cb) => {
                return async.each(csAppsDesc, (app, createCallback) => {
                    if (_.find(existing_apps, { id: app.id })) {
                        return setImmediate(createCallback);
                    }

                    return this.createApplication(app, {
                        handler: (val) => createCallback(null, val),
                        errorHandler: (err) => createCallback(err)
                    });
                }, (err) => {
                    return add_cb(err);
                });
            }
        ], (err) => {
            if (err) {
                return errorHandler(err);
            }

            return handler();
        });
    }

    deleteApplication(name, cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        return this.updateApplication(name, { tags: { constraints: {} }, count: 0 }, {
            errorHandler,
            handler: () => {
                const self = this;
                function safeDeleteRC(delete_cb) {
                    return request({
                        uri: `${self.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers/${name}`,
                        method: 'GET'
                    }, (err, res, body) => {
                        if(err || res.statusCode !== 200) {
                            return delete_cb(`Failed to retreive existing replica controller for application ${name}`);
                        }

                        const rc = JSON.parse(body);
                        const podCount = _.get(rc, ['status', 'replicas'], Number.MAX_SAFE_INTEGER);

                        // eslint-disable-next-line no-console
                        console.log('Surviving pod count: ' + podCount);

                        if(podCount > 0) {
                            return delete_cb(`There are still pods: ${podCount}...waiting 4 seconds for pods to delete`);
                        }

                        // eslint-disable-next-line no-console
                        console.log('Making final delete.');

                        return async.parallel([
                            (cb) => {
                                return request({
                                    uri: `${self.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers/${name}`,
                                    method: 'DELETE'
                                }, (err, res) => {
                                    if(err || res.statusCode !== 200) {
                                        return cb(`Failed to delete replication controller: ${name}`);
                                    }

                                    return cb();
                                });
                            },
                            (cb) => {
                                self.deleteService(name, cb);
                            }
                        ], (err) => {
                            if(err) {
                                return delete_cb(err);
                            }

                            return delete_cb();
                        });
                    });
                }

                return async.retry({ times: 5, interval: 4000 }, safeDeleteRC, (err) => {
                    if(err) {
                        return errorHandler(err);
                    }

                    return handler();
                });
            }
        });
    }

    getHost(host, cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        return this.getHosts((err, hosts) => {
            if (err) {
                return errorHandler(err);
            }

            if (!hosts[host]) {
                return errorHandler(`The requested host does not exist: ${host}`);
            }

            return handler(hosts[host]);
        });
    }

    updateHost(host_id, tags, cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        if (!tags) {
            return errorHandler('You must provide tags to update on the host');
        }

        return this.getHost(host_id, (err, host) => {
            if (err) {
                return errorHandler(err);
            }

            tags = flatten({ tags: tags });
            const current_tags = flatten({
                tags: host.tags
            });

            // label values must begin and end with an alphanumeric character
            // and only support [_.-] as special characters in between
            const label_regex = /^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$/;

            const tag_labels = _.reduce(_.pickBy(_.merge(current_tags, tags), (value) => {
                // filter out invalid label values
                return label_regex.test(value);
            }), (acc, val, key) => {
                // null values are deleted in patch request
                acc[key] = tags[key] ? val : null;
                return acc;
            }, {});

            return request({
                uri: `${this.k8sApiUrl}/api/v1/nodes/${host_id}`,
                method: 'PATCH',
                body: JSON.stringify({
                    metadata: {
                        labels: tag_labels,
                        namespace: ''
                    }
                }),
                headers: {
                    'Content-Type': 'application/strategic-merge-patch+json',
                    'Accept': 'application/json'
                }
            }, (err, res, body) => {
                if(err || res.statusCode !== 200) {
                    return errorHandler(err || body);
                }

                return handler(body);
            });
        });
    }

    //Look here!
    //Use os module to get info about master!
    getHosts(cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        return async.parallel({
            nodes: (cb) => {
                return request(`${this.k8sApiUrl}/api/v1/nodes`,
                    (err, res, body) => {
                        if(err || res.statusCode !== 200) {
                            return cb('Failed to retrive k8s nodes inside getHosts call');
                        }

                        return cb(null, util.safeParse(body));
                    });
            },
            services: (cb) => {
                return request(`${this.k8sApiUrl}/api/v1/services`,
                    (err, res, body) => {
                        if(err || res.statusCode !== 200) {
                            return cb('Failed to retrive k8s services inside getHosts call');
                        }

                        return cb(null, util.safeParse(body));
                    });
            },
            containers: (cb) => {
                return this.getContainers((err, containers) => {
                    return cb(err, containers);
                });
            }
        }, (err, responses) => {
            if(err) {
                return errorHandler(err);
            }

            const containers = responses.containers;
            const k8sNodes = responses.nodes.items;
            const k8sServices = responses.services.items;


            const k8sCoreService = _.find(k8sServices, (s) => {
                return _.get(s, 'metadata.name') === 'kubernetes';
            });

            const containersByNode = _.flow(
                _.partial(_.map, _, (container) => {
                    const csNodeName =
                        _.flow(
                            _.partial(_.filter, _, (n) => {
                                return _.get(n, ['metadata', 'name']) === container.host;
                            }),
                            _.first,
                            (node) => {
                                return _.get(node, ['metadata', 'labels', 'cs-node-id'], 'NOT FOUND!');
                            })(k8sNodes);

                    return[csNodeName, container];
                }),
                _.partial(_.reduce, _, (result, nodeContainer) => {
                    const[nodeName, container] = nodeContainer;
                    const containersForNode = _.get(result, nodeName, []);
                    return _.set(result, nodeName, _.concat(containersForNode, container));
                }, {}))(containers);

            const csFollowers = _.map(k8sNodes, _.flow(
                Translator.csHostFromK8SNode,
                (csNode) => _.merge(csNode, {
                    'metadata': {
                        'kubernetes': {
                            'version':'1.3.8'
                        }
                    },
                    'mode': 'follower',
                    'praetor': {
                        leader: false,
                        leader_eligible: false,
                    },
                    'containers': containersByNode[csNode.id] || []
                })));

            const leaderId = process.argv[6] || os.hostname();
            const leaderName = os.hostname();

            const csLeader = {
                'id': leaderId,
                'host_name': leaderName,

                'metadata': {
                    'kubernetes': {
                        'version':'1.3.8'
                    }
                },

                tags: {
                    host: leaderId,
                    host_name: leaderName
                },

                praetor: {
                    leader: true,
                    leader_eligible: true,
                },

                mode: 'leader',

                start_time: Date.parse(_.get(k8sCoreService, 'metadata.creationTimestamp')),

                address: {
                    private: this.k8sApiIP,
                    public: this.k8sApiIP
                },

                containers: []
            };

            const keyed = _.keyBy(_.concat(csLeader, csFollowers), 'id');

            return handler(keyed);
        });
    }

    setDistributedKey(key, value, cb) {
        const req = {
            uri: `${this.etcdApiUrl}/v2/keys/${key}`,
            method: 'PUT',
            form: {
                value: JSON.stringify(value)
            }
        };

        request(req, cb);
    }

    getDistributedKey(key, cb, isWaiting) {
        isWaiting = isWaiting ? true: false;

        const req = {
            uri: `${this.etcdApiUrl}/v2/keys/${key}?wait=${isWaiting}`,
            method: 'GET',
            json: true
        };

        request(req, (err, resp, body) => {
            if(err) {
                cb(err);
            } else{
                cb(null, util.safeParse(_.get(body, ['node', 'value'])));
            }
        });
    }

    subscribeDistributedKey(pattern) {
        const ee = new EventEmitter();

        const self = this;

        // Can't use a => here because the function is self-referential.
        function handler(err, value) {
            if(err) {
                ee.emit('error', JSON.stringify({
                    error: err.message
                }));
            } else{
                ee.emit('message', value);
            }

            self.getDistributedKey(pattern, handler, true);
        }

        this.getDistributedKey(pattern, handler, true);

        return ee;
    }

}

module.exports = KubernetesApi;
