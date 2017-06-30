'use strict';

const _ = require('lodash');
const async = require('async');
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
        this.getHosts((host) => {
            this.getApplications((apps) => {
                _.each(apps, (app) => {
                    this.enforceConstraints(host, app);
                });
            });
        });
    }

    enforceConstraints(hosts, app) {
        const constraints =
            _.get(app, ['tags', 'constraints'], {});

        const perHost = parseInt(_.get(constraints, ['per_host'], 0));
        const min = parseInt(_.get(constraints, ['min'], 0));
        const max = parseInt(_.get(constraints, ['max'], Number.MAX_SAFE_INTEGER));

        const running = _.size(_.get(app, 'containers', []));

        const followers = _.filter(_.values(hosts), (h) => h.mode === 'follower');
        const followerCount = _.size(followers);
        const neededReplicas = perHost > 0 ? followerCount * perHost : running;

        const newCount =
            neededReplicas > max ? max :
            neededReplicas < min ? min :
            neededReplicas;

        if(newCount != running) {
            this.updateApplication(app.id, { tags: { constraints: {} }, count: newCount }, {
                errorHandler: (err) => {
                    // eslint-disable-next-line no-console
                    console.log(`Error updating application when enforcing constraints for ${app.id}: ${err}`);
                },
                handler: () => {
                    // eslint-disable-next-line no-console
                    console.log(`Enforcing constraints for ${app.id} updated count to ${newCount}`);
                }
            });
        }
    }

    getApplications(cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

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
            const hosts = responses.hosts;

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

    getContainers(cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        return request(`${this.k8sApiUrl}/api/v1/pods`,
            (err, res, body) => {
                if(err || res.statusCode !== 200) {
                    return errorHandler('Failed to retrive k8s pods inside getContainers call');
                }

                const pods = _.map(util.safeParse(body).items, pod => {
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

                return handler(pods);
            });
    }

    createContainers(appId, containerConfig, cb) {
        const{ errorHandler } = this.sanitizeCallback(cb);

        return this.getApplications((err, apps) => {
            if(err) {
                return errorHandler(err);
            }

            const app = _.first(_.filter(apps, (v, k) => k == appId));

            if(!app) {
                return errorHandler(Error(`Couldn't find application with id ${appId} when in KubernetesApi::createContainer!`));
            }

            const containerCount = _.size(_.get(app, 'containers'));
            const newCount = containerCount + parseInt(containerConfig.count);

            this.updateApplication(appId, _.merge(
                containerConfig,
                {count: newCount}
            ), cb);
        });
    }

    scaleDownContainers(app_id, count, cb) {
        return this.scaleContainers({
            app_id: app_id,
            count: count,
            type: 'down'
        }, cb);
    }

    scaleUpContainers(app_id, count, cb) {
        return this.scaleContainers({
            app_id: app_id,
            count: count,
            type: 'up'
        }, cb);
    }

    scaleContainers(options, cb) {
        const{ errorHandler } = this.sanitizeCallback(cb);

        const app_id = options.app_id;
        const type = options.type;
        const count = parseInt(options.count || 1);

        return this.getApplications((err, apps) => {
            if(err) {
                return errorHandler(err);
            }

            const app = _.first(_.filter(apps, (v, k) => k === app_id));

            if(!app) {
                return errorHandler(Error(`Couldn't find application with id ${app_id} when in KubernetesApi::scaleContainers!`));
            }

            const current_count = _.size(_.get(app, 'containers'));
            const newCount = type === 'up' ? current_count + count : current_count - count;

            return this.updateApplication(app_id, {
                count: newCount
            }, cb);
        });
    }

    deleteContainer(appId, containerId, cb) {
        const{ errorHandler } = this.sanitizeCallback(cb);

        return this.getApplications((err, apps) => {
            if(err) {
                return errorHandler(err);
            }

            const app = _.first(_.filter(apps, (v, k) => k == appId));

            if(!app) {
                return errorHandler(Error(`Couldn't find application with id ${appId} when in KubernetesApi::createContainer!`));
            }

            const containerCount = _.size(_.get(app, 'containers'));
            const newCount = containerCount - 1;

            return this.updateApplication(appId, {count: newCount}, cb);
        });
    }

    updateApplication(appId, csAppDesc, cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        return this.getApplications((err, apps) => {
            if(err) {
                return errorHandler(err);
            }

            const initializedConstraints = {
                tags: {
                    constraints: {
                        max: Number.MAX_SAFE_INTEGER,
                        min: 0,
                        per_host: 0
                    }
                }
            };

            const existingApp = apps[appId];

            if(!existingApp) {
                return errorHandler(`Couldn't find application with id ${appId} when in KubernetesApi::updateApplication!`);
            }

            // If we have an empty tags object, UI logic seems to dictate
            // that the tags get reinitialized.
            const newApp = _.isNil(_.get(csAppDesc, 'tags.constraints'))
                ? _.merge(csAppDesc, initializedConstraints)
                : csAppDesc;

            const k8sDesc =
                Translator.csApplicationToK8SReplicationController(_.merge(existingApp, newApp));

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
                    return errorHandler('Failed to update application');
                }

                return handler(body);
            });
        });

    }

    createApplication(csAppDesc, cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        const containerPort = csAppDesc.container_port ? csAppDesc.container_port : _.random(11024, 22047);

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

        const k8sAppDesc = Translator.csApplicationToK8SReplicationController(_.merge(csAppDesc, {
            container_port: containerPort,
            env_vars: {
                CS_ORCHESTRATOR: 'kubernetes',
                PORT: containerPort
            }
        }));

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

    createApplications(csAppsDesc, cb) {
        const{ handler, errorHandler } = this.sanitizeCallback(cb);

        return async.each(csAppsDesc, (app, createCallback) => {
            return this.createApplication(app, {
                handler: (val) => createCallback(null, val),
                errorHandler: (err) => createCallback(err)
            });
        }, (err) => {
            if(err) {
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

                        console.log('Surviving pod count: ' + podCount);

                        if(podCount > 0) {
                            return delete_cb(`There are still pods: ${podCount}...waiting 4 seconds for pods to delete`);
                        }

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
                        start_time: Date.now()
                    },
                    'containers': containersByNode[csNode.id] || []
                })));

            const csLeader = {
                'id': process.argv[6] || os.hostname(),
                'host_name': os.hostname(),

                'metadata': {
                    'kubernetes': {
                        'version':'1.3.8'
                    }
                },

                praetor: {
                    leader: true,
                    leader_eligible: true,
                    start_time: Date.now()
                },

                mode: 'leader',

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

