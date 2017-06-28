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
            console.log(`There was an error in the K8S Api Bridge: ${err}`);
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
        const req = {
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/services`,
            method: 'POST',
            json: true,
            body: KubernetesApi.k8sServiceDescriptionFor(k8sAppDesc, k8sPorts, discoveryPorts)
        };

        return request(req, cb);
    }

    deleteService(app_name, cb) {
        const service_name = `ser${_.join(_.take(String(encode(app_name)), 19), '')}`;

        const req = {
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/services/${service_name}`,
            method: 'DELETE',
        };

        return request(req, (err, res) => {
            if (err || res.statusCode !== 200) {
                return cb(`Failed to delete service: ${service_name}`);
            }

            return cb(null, res);
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
            this.updateApplication(app.id, { count: newCount }, {
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

        cb = this.sanitizeCallback(cb);
        const{ handler, errorHandler } = cb;

        const ifSuccessfulResponse =
            _.partialRight(this.ifSuccessfulResponse, {
                errorHandler: errorHandler,
                method: 'getApplications/'
            });

        //Get the replication controllers
        request(`${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers`,
            ifSuccessfulResponse((replicationControllers) => {
                replicationControllers = util.safeParse(replicationControllers.body);

                //And the services (for discovery).
                request(`${this.k8sApiUrl}/api/v1/namespaces/default/services`,
                    ifSuccessfulResponse((services) => {
                        services = util.safeParse(services.body);

                        this.getHosts((err, hosts) => {

                            // In cases where we rely on our own api methods in building a response,
                            // we do use custom logic to propogate errors to the
                            // user's handler rather than our own.
                            if(err) {
                                errorHandler(err); return;
                            }

                            const k8sItems = _.get(replicationControllers, 'items', []);
                            const k8sServices = _.get(services, 'items', []);

                            const allContainers = _.concat.apply(null, _.map(hosts, (h) => {
                                const augmentedContainers =
                                    _.map(_.get(h, 'containers'), (c) => {
                                        return _.merge(c, {
                                            host: h.id
                                        });
                                    });
                                return augmentedContainers;
                            }));

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

                            handler(csApps);
                        });
                    }));
            }));
    }

    createContainers(appId, containerConfig, cb) {

        cb = this.sanitizeCallback(cb);

        this.getApplications((err, apps) => {

            const{ errorHandler } = cb;

            if(err) {
                errorHandler(err); return;
            }

            const app = _.first(_.filter(apps, (v, k) => k == appId));

            if(!app) {
                errorHandler(Error(`Couldn't find application with id ${appId} when in KubernetesApi::createContainer!`));
                return;
            }

            const containerCount = _.size(_.get(app, 'containers'));
            const newCount = containerCount + parseInt(containerConfig.count);

            this.updateApplication(appId, _.merge(
                containerConfig,
                {count: newCount}
            ), cb);

        });
    }

    deleteContainer(appId, containerId, cb) {
        cb = this.sanitizeCallback(cb);
        const{ errorHandler } = cb;

        this.getApplications((err, apps) => {

            if(err) {
                errorHandler(err);
                return;
            }

            const app = _.first(_.filter(apps, (v, k) => k == appId));

            if(!app) {
                errorHandler(Error(`Couldn't find application with id ${appId} when in KubernetesApi::createContainer!`));
                return;
            }

            const containerCount = _.size(_.get(app, 'containers'));
            const newCount = containerCount - 1;

            this.updateApplication(appId, {count: newCount}, cb);
        });
    }

    updateApplication(appId, csAppDesc, cb) {

        cb = this.sanitizeCallback(cb);
        const{ handler, errorHandler } = cb;

        const ifSuccessfulResponse =
            _.partialRight(this.ifSuccessfulResponse, {
                errorHandler: errorHandler,
                method: 'updateApplication'
            });

        this.getApplications((err, apps) => {

            if(err) {
                errorHandler(err);
                return;
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

            const existingApp = _.flow(
                _.first,
                _.partial(_.filter, _, (v, k) => k == appId))(apps);
                //_.partial(_.omit, _, ['spec', 'template', 'spec', 'containers', 0, 'lifecycle']))(apps);

            if(!existingApp) {
                errorHandler(Error(`Couldn't find application with id ${appId} when in KubernetesApi::updateApplication!`));
                return;
            }

            // If we have an empty tags object, UI logic seems to dictate
            // that the tags get reinitialized.
            const newApp = _.isNil(_.get(csAppDesc, ['tags', 'constraints'])) ?
                _.merge(csAppDesc, initializedConstraints) : csAppDesc;

            const k8sDesc =
                Translator.csApplicationToK8SReplicationController(_.merge(existingApp, newApp));


            request({
                uri: `${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers/${appId}`,
                method: 'PATCH',
                body: k8sDesc,
                json: true,
                headers: {
                    'Content-Type': 'application/merge-patch+json'
                }
            }, ifSuccessfulResponse(handler));

        });

    }

    createApplication(csAppDesc, cb) {

        cb = this.sanitizeCallback(cb);
        const{ handler, errorHandler } = cb;

        const ifSuccessfulResponse =
            _.partialRight(this.ifSuccessfulResponse, {
                errorHandler: errorHandler,
                method: 'createApplication'
            });

        const hostPort = _.cond([
            [(containerPort, hostPort, preferRandom) => preferRandom,
                () => _.random(1, 65535)],
            [(containerPort, hostPort, preferRandom) => !preferRandom && !hostPort && containerPort,
                (containerPort) => containerPort],
            [(containerPort, hostPort, preferRandom) => !preferRandom && hostPort,
                (containerPort, hostPort) => hostPort],
            [_.constant(true), () => _.random(1, 65535)]
        ])(csAppDesc.container_port, csAppDesc.host_port, csAppDesc.random_host_port || false);

        const containerPort = csAppDesc.container_port ? csAppDesc.container_port : hostPort;

        const initialK8SAppDesc = Translator.csApplicationToK8SReplicationController(_.merge(csAppDesc, {
            host_port: hostPort,
            container_port: containerPort,
            env_vars: { CS_ORCHESTRATOR: 'kubernetes' }
        }));

        const k8sAppDesc = initialK8SAppDesc;

        request({
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers`,
            method: 'POST',
            body: k8sAppDesc,
            json: true
        }, ifSuccessfulResponse(() => {

            const ports = _.get(k8sAppDesc, ['spec', 'template', 'spec', 'containers', 0, 'ports'], []);

            if(_.size(ports) === 0) {
                handler();
            } else{
                this.createService(k8sAppDesc, ports, [], ifSuccessfulResponse(handler));
            }

        }));

    }

    createApplications(csAppsDesc, cb) {
        cb = this.sanitizeCallback(cb);
        const{ handler, errorHandler } = cb;

        return async.each(csAppsDesc, (app, createCallback) => {
            return this.createApplication(app, createCallback);
        }, (err) => {
            if (err) {
                return errorHandler(err);
            }

            return handler();
        });
    }

    deleteApplication(name, cb) {
        cb = this.sanitizeCallback(cb);
        const{ handler, errorHandler } = cb;

        const ifSuccessfulResponse =
            _.partialRight(this.ifSuccessfulResponse, {
                errorHandler: errorHandler,
                method: 'deleteApplication'
            });

        this.updateApplication(name, { tags: { constraints: {} }, count: 0 }, (err, res) => {

            if(err) return errHandler(err);

            const self = this;
            function safeDeleteRC() {

                request({
                    uri: `${self.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers/${name}`,
                    method: 'GET'
                }, ifSuccessfulResponse((res) => {

                    const rc = JSON.parse(res.body);

                    const podCount = _.get(rc, ['status', 'replicas'], Number.MAX_SAFE_INTEGER);

                    console.log("Surviving pod count: " + podCount);

                    if(podCount === 0) {

                        console.log("Making final delete.");

                        async.parallel([
                            (cb) => {
                                return request({
                                    uri: `${self.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers/${name}`,
                                    method: 'DELETE'
                                }, (err, res) => {
                                    if (err || res.statusCode !== 200) {
                                        return cb(`Failed to delete replication controller: ${name}`);
                                    }

                                    return cb(null, res);
                                });
                            },
                            (cb) => {
                                self.deleteService(name, cb);
                            }
                        ], (err) => {
                            if (err) {
                                return errorHandler(err);
                            }

                            return handler();
                        });

                    } else {
                        console.log("Trying again in 4 sec");
                        setTimeout(safeDeleteRC, 4000);
                    }
                }));

            }

            // wait one second by default, so it always has
            // a chance to scale down pods if they do exist
            setTimeout(safeDeleteRC, 1000);
        });

    }

    //Look here!
    //Use os module to get info about master!
    getHosts(cb) {
        cb = this.sanitizeCallback(cb);
        const{ handler, errorHandler } = cb;
        const ifSuccessfulResponse =
            _.partialRight(this.ifSuccessfulResponse, {
                errorHandler: errorHandler,
                method: 'getHosts'
            });

        request(`${this.k8sApiUrl}/api/v1/nodes`, ifSuccessfulResponse((nodes) => {
            nodes = util.safeParse(nodes.body);

            request(`${this.k8sApiUrl}/api/v1/pods`, ifSuccessfulResponse((pods) => {
                pods = util.safeParse(pods.body);

                const k8sNodes = nodes.items;
                const k8sPods = pods.items;

                const containersByNode = _.flow(
                    _.partial(_.map, _, (pod) => {
                        const nodeName = _.get(pod, ['spec', 'nodeName']);

                        const csNodeName =
                            _.flow(
                                _.partial(_.filter, _, (n) => {
                                    return _.get(n, ['metadata', 'name']) === nodeName;
                                }),
                                _.first,
                                (node) => {
                                    return _.get(node, ['metadata', 'labels', 'cs-node-id'], 'NOT FOUND!');
                                })(k8sNodes);

                        const k8sContainer = _.get(pod, 'spec');
                        const status = _.get(pod, ['status', 'phase']);
                        const containerID = _.replace(_.get(pod, ['status', 'containerStatuses', 0, 'containerID']), 'docker://', '');

                        const csContainer = _.merge(
                            Translator.csApplicationFromK8SPodSpec(k8sContainer),
                            {
                                status: status === 'Running' ? 'loaded' : 'unloaded',
                                container_id: _.replace(containerID, 'docker://', ''),
                                id: containerID
                            });

                        return[csNodeName, csContainer];
                    }),
                    _.partial(_.reduce, _, (result, nodeContainer) => {
                        const[nodeName, container] = nodeContainer;
                        const containersForNode = _.get(result, nodeName, []);
                        return _.set(result, nodeName, _.concat(containersForNode, container));
                    }, {}))(k8sPods);

                const csFollowers = _.map(k8sNodes, _.flow(
                    Translator.csHostFromK8SNode,
                    (csNode) => _.merge(csNode, {
                        'metadata': {
                            'containership': {
                                'version': '0.0.0'
                            },
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
                        'containership': {
                            'version': '0.0.0'
                        },
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

                handler(keyed);

            }));
        }));
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

