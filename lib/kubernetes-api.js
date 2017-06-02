'use strict';

const _ = require('lodash');
const os = require('os');
const request = require('request');
const EventEmitter = require('events');

const Translator = require('@containership/containership.k8s.translator');
const ApiImplementation  = require('@containership/containership.abstraction.api');

const util = require('./util');

const DEFAULT_K8S_API_URL = 'http://localhost:8080';
const DEFAULT_ETCD_API_URL = 'http://localhost:2379';

class KubernetesApi extends ApiImplementation {

    constructor(k8sApiIP, k8sApiPort) {
        super();

        this.k8sApiIP = k8sApiIP;
        this.k8sApiUrl = k8sApiIP ? `http://${k8sApiIP}:${k8sApiPort}` : DEFAULT_K8S_API_URL;
        this.etcdApiUrl = k8sApiIP ? `http://${k8sApiIP}:2379` : DEFAULT_ETCD_API_URL;
    }

    static k8sServiceDescriptionFor(appDesc, ports, discoveryPorts) {
        const appName = _.get(appDesc, ['metadata', 'name']);

        return {
            kind: 'Service',
            metadata: {
                name: `${_.join(_.take(strEncode(appName), 15), '')}-service` // Limit to 24 chars
            },
            spec: {
                type: 'NodePort',
                ports: _.map(ports, (port, idx) => {
                    return _.merge({
                        port: port.containerPort
                    }, discoveryPorts[idx] ? {nodePort: discoveryPorts[idx]} : {});
                }),
                selector: {
                    app: _.get(appDesc, ['metadata', 'name'])
                }
            }
        };
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
        const constraints = _.get(app, ['tags', 'constraints'], {});

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
            this.updateApplication(app.id, {count: newCount}, (x) => {
                // eslint-disable-next-line no-console
                console.log(JSON.stringify(x));
            });
        }
    }

    getApplications(cb) {
        //Get the replication controllers
        request(`${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers`, util.ifSuccessfulResponse((replicationControllers) => {
            //And the services (for discovery).
            request(`${this.k8sApiUrl}/api/v1/namespaces/default/services`, util.ifSuccessfulResponse((services) => {
                this.getHosts((hosts) => {
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

                    cb(csApps);
                });
            }));
        }));
    }

    createService(k8sAppDesc, k8sPorts, discoveryPorts, cb) {
        // eslint-disable-next-line no-console
        console.log('Creating service!');

        const req = {
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/services`,
            method: 'POST',
            json: true,
            body: KubernetesApi.k8sServiceDescriptionFor(k8sAppDesc, k8sPorts, discoveryPorts)
        };

        request(req, cb);
    }

    createContainers(appId, containerConfig, cb) {

        this.getApplications((apps) => {
            const app = _.first(_.filter(apps, (v, k) => k == appId));

            if(app) {
                const containerCount = _.size(_.get(app, 'containers'));
                const newCount = containerCount + parseInt(containerConfig.count);

                this.updateApplication(appId, _.merge(
                    containerConfig,
                    {count: newCount}
                ), cb);
            }
        });
    }

    deleteContainer(appId, containerId, cb) {
        this.getApplications((apps) => {
            const app = _.first(_.filter(apps, (v, k) => k == appId));

            if(app) {
                const containerCount = _.size(_.get(app, 'containers'));
                const newCount = containerCount - 1;

                this.updateApplication(appId, {count: newCount}, cb);
            }
        });
    }

    updateApplication(appId, csAppDesc, cb) {
        this.getApplications((apps) => {

            const initializedConstraints = {tags: {constraints: {max: Number.MAX_SAFE_INTEGER, min: 0, per_host: 0}}};

            const existingApp = _.first(_.filter(apps, (v, k) => k == appId));

            const newApp = _.has(csAppDesc, 'tags') && _.isEmpty(csAppDesc.tags) ?
                _.merge(csAppDesc, initializedConstraints) : csAppDesc;


            //A hack to fix the stange tags handling on UI.
            const k8sDesc = Translator.csApplicationToK8SReplicationController(_.merge(existingApp, newApp));

            request({
                uri: `${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers/${appId}`,
                method: 'PATCH',
                body: k8sDesc,
                json: true,
                headers: {
                    'Content-Type': 'application/merge-patch+json'
                }
            }, (err, res, body) => {

                //Create a service for this application.
                if(!err) {
                    cb(res.statusCode, body);
                } else {
                    // eslint-disable-next-line
                    console.error(JSON.stringify(err));
                    // eslint-disable-next-line
                    console.error(res);
                }
            });

        });

    }

    createApplication(csAppDesc, cb) {

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

        const k8sAppDesc = Translator.csApplicationToK8SReplicationController(_.merge(csAppDesc, {
            host_port: hostPort,
            container_port: containerPort,
            env_vars: { CS_ORCHESTRATOR: 'kubernetes' }
        }));

        request({
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers`,
            method: 'POST',
            body: k8sAppDesc,
            json: true
        }, (err, res, body) => {
            // Create a service for this application.
            if(!err) {

                const ports = _.get(k8sAppDesc, ['spec', 'template', 'spec', 'containers', 0, 'ports']);

                if(_.size(ports) > 0) {
                    this.createService(
                        k8sAppDesc,
                        ports,
                        [],
                        (err, res, body) => {
                            if (err) {
                                // eslint-disable-next-line no-console
                                console.error(err);
                            }

                            return cb(res.statusCode, body);
                        }
                    );
                } else {
                    cb(null, res.statusCode, body);
                }

            } else {
                // eslint-disable-next-line
                console.log(JSON.stringify(err));

                // eslint-disable-next-line
                console.log(res);

                return cb(err, body);
            }
        });
    }

    deleteApplication(name, cb) {
        request({
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers/${name}`,
            method: 'DELETE'
        }, (err, res, body) => {
            if (err) {
                // eslint-disable-next-line no-console
                console.error(err);
            }

            return cb(res.statusCode, body);
        });
    }

    //Look here!
    //Use os module to get info about master!
    getHosts(cb) {
        request(`${this.k8sApiUrl}/api/v1/nodes`, util.ifSuccessfulResponse((nodes) => {

            request(`${this.k8sApiUrl}/api/v1/pods`, util.ifSuccessfulResponse((pods) => {

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
                                _.partial(_.first, _),
                                (node) => {
                                    return _.get(node, ['metadata', 'labels', 'cs-node-id'], 'NOT FOUND!');
                                })(k8sNodes);

                        const k8sContainer = pod;
                        const status = _.get(pod, ['status', 'phase']);
                        const containerID = _.replace(_.get(pod, ['status', 'containerStatuses', 0, 'containerID']), 'docker://', '');

                        const csContainer = _.merge(
                            Translator.csApplicationFromK8SPodSpec(k8sContainer),
                            {
                                status: status === 'Running' ? 'loaded' : 'unloaded',
                                container_id: _.replace(containerID, 'docker://', ''),
                                id: containerID
                            });

                        return [csNodeName, csContainer];
                    }),
                    _.partial(_.reduce, _, (result, nodeContainer) => {
                        const [nodeName, container] = nodeContainer;
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

                cb(_.keyBy(_.concat(csLeader, csFollowers), 'id'));
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
            } else {
                cb(null, util.safeParse(_.get(body, ['node', 'value'])));
            }
        });
    }

    subscribeDistributedKey(pattern) {
        const ee = new EventEmitter();

        const self = this;

        // Can't use a => here because the function is recursive.
        function handler(err, value) {
            if(err) {
                ee.emit('error', JSON.stringify({
                    error: err.message
                }));
            } else {
                ee.emit('message', value);
            }

            self.getDistributedKey(pattern, handler, true);
        }

        this.getDistributedKey(pattern, handler, true);

        return ee;
    }

}

module.exports = KubernetesApi;

