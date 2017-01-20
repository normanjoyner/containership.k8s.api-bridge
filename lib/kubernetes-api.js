const _ = require('lodash');
const os = require('os');
const request = require('request');
const encode = require('hashcode').hashCode().value;

const util = require('./util');
const { ApiImplementation } = require('containership.plugin.v2');

const DEFAULT_K8S_API_URL = "http://localhost:8080";

const EVT_MARK_PORT_USED = "MARK_PORT_USED";

var state = {};

// State management 

function dispatch(event) {
    newState = stateReducer(state, event);
    //Write to file.
    state = newState;
}

function stateReducer(currentState, event) {
    return (() => {
        switch(event.type) {
            case EVT_MARK_PORT_USED:
                const usedPorts = _.get(currentState, 'usedPorts', []);
                return _.set(currentState, 'usedPorts', _.concat(usedPorts, event.port))
        }
    })(event);
}

// ----


//Some hacks to use complex keys in JS dicts.

function registerLookup(object, hash) {
    hash = hash ? hash : strEncode(object);
    KubernetesApi.CS_TO_K8S_HASH_LOOKUP[hash] = object;
}

function objectFor(hash) {
    return KubernetesApi.CS_TO_K8S_HASH_LOOKUP[hash]; 
}

function buildFromLookup(storage, k, v) {
    const hash = strEncode(k);
    storage[hash] = v;
    registerLookup(k, hash);
}

// ----

// Some generic helpers.

function strEncode(v) {
    return String(encode(v));
}

function randomNotTaken(lower, upper, taken) {
    var v;

    while(!v || _.includes(taken, v)) {
        v = _.random(lower, upper);
    }

    return v;
}

// ----

class KubernetesApi extends ApiImplementation {

    constructor(k8sApiUrl) {
        super();
        this.k8sApiUrl = k8sApiUrl || DEFAULT_K8S_API_URL;
    }

    static k8sServiceDescriptionFor(appDesc, ports, discoveryPorts) {
        const appName = _.get(appDesc, ['metadata', 'name']);

        return {
            kind: 'Service',
            metadata: {
                name: `${appName}-service`
            },
            spec: {
                type: 'LoadBalancer',
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

    static k8sToCSApp(k8sApp) {
        return _.merge({
            respawn: false,
            privileged: false,
            network_mode: 'bridge'
        },
            _.reduce(KubernetesApi.CS_TO_K8S_APPLICATION_MAPPING, (csApp, k8sPaths, csKey) => {

                //It doesn't matter which k8s path we grab, they all lead to the same value.
                const k8sPath = _.first(k8sPaths);

                //FIX ME!
                const csHashKey = strEncode([csKey]);
                const conversionFn = KubernetesApi.K8S_TO_CS_CONVERSIONS[csHashKey];

                const baseV = _.get(k8sApp, k8sPath);
                const v = conversionFn && baseV ? conversionFn(baseV) : baseV;

                return v ? _.set(csApp, csKey, v): csApp;
            }, {}));
    }

    static csToK8SApp(csApp) {
        const name = csApp.id;
        const count = csApp.count || 0;

        return _.reduce(KubernetesApi.CS_TO_K8S_APPLICATION_MAPPING, (k8sApp, k8sPaths, csKey) => {
            const conversionFn = KubernetesApi.CS_TO_K8S_CONVERSIONS[csKey];

            const baseV = csApp[csKey];
            const v = conversionFn && baseV ? conversionFn(baseV) : baseV;

            return v ? 
                _.merge(k8sApp, 
                    _.reduce(k8sPaths, (setK8SPaths, path) => {
                        return _.set(setK8SPaths, _.concat(['spec', 'template', 'spec', 'containers', 0], path), v);
                    }, {})) :
                k8sApp;
        }, {
            "kind": "ReplicationController", 
            'metadata': {'name': name},
            "spec": {
                'selector': {
                    'app': name
                },
                'template': {
                    'metadata': {
                        'labels': {
                            'app': name
                        }
                    }
                },
                "replicas": count
            }
        });
    }

    static k8sToCSNode(k8sNode) {
        return _.reduce(KubernetesApi.CS_TO_K8S_NODE_MAPPING, (csNode, k8sPath, hashCSPath) => {
            const csPath = objectFor(hashCSPath);
            const conversionFn = KubernetesApi.K8S_TO_CS_CONVERSIONS[hashCSPath];
            const value = conversionFn ? conversionFn(_.get(k8sNode, k8sPath)) : _.get(k8sNode, k8sPath);
            return value ? _.set(csNode, csPath, value) : csNode;
        }, {});
    }

    getClusterState() {
        // TODO
    }

    deleteCluster() {
        // TODO
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
                        const appName = _.get(container, 'id');
                        const containers = _.get(result, appName, []);
                        return _.set(result, appName, _.concat(containers, container));
                    }, {});

                    const csItems = _.map(k8sItems, _.flow(
                        _.partial(_.get, _, ['spec', 'template', 'spec', 'containers', 0]),
                        KubernetesApi.k8sToCSApp,
                        (csApp) => {
                            //Augment the app with the discovwery port.
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
                            return _.set({}, item.id, _.merge(item, {
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

                console.log("Have new count: " + newCount);

                this.updateApplication(appId, _.merge(
                    containerConfig,
                    {count: newCount}
                ), cb);
            }
        });
    }

    updateApplication(appId, csAppDesc, cb) {
        const k8sDesc = _.omit(KubernetesApi.csToK8SApp(csAppDesc), [['spec', 'template']]);

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
                console.log(JSON.stringify(err));
                console.log(res);
            }
        });
    }

    createApplication(csAppDesc, cb) {

        const hostPort = _.cond([
            [(containerPort, hostPort, preferRandom) => preferRandom, 
                () => randomNotTaken(1, 65535, state.usedPorts)],
            [(containerPort, hostPort, preferRandom) => !preferRandom && !hostPort && containerPort,
                (containerPort) => containerPort],
            [(containerPort, hostPort, preferRandom) => !preferRandom && hostPort,
                (containerPort, hostPort) => hostPort],
            [_.constant(true), () => randomNotTaken(1, 65535, state.unusedPorts)]
        ])(csAppDesc.container_port, csAppDesc.host_port, csAppDesc.random_host_port || true);
            
        const containerPort = csAppDesc.container_port ? csAppDesc.container_port : hostPort;

        /*dispatch({
            type: EVT_MARK_PORT_USED,
            port: hostPort
        });*/

        const k8sAppDesc = KubernetesApi.csToK8SApp(_.merge(csAppDesc, {
            //host_port: hostPort, 
            container_port: containerPort
        }));

        request({ 
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers`,
            method: 'POST',
            body: k8sAppDesc,
            json: true
        }, (err, res, body) => { 
            //Create a service for this application.
            if(!err) {
                const ports = _.get(k8sAppDesc,['spec', 'template', 'spec', 'containers', 0, 'ports']);
                if(_.size(ports) > 0) {
                    this.createService(
                        k8sAppDesc, 
                        ports, 
                        [],
                        (err, res, body) => { 
                            return cb(res.statusCode, body); 
                        }
                    );
                } else {
                    cb(res.statusCode, body);
                }
            } else {
                console.log(JSON.stringify(err));
                console.log(res);
            }
        });
    }

    deleteApplication(name) {
        request({
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/${name}`,
            method: 'DELETE'
        }, util.ifSuccessfulResponse((resp) => {
            console.log(`Deleting ${name}: ${resp}.`);
        }));
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
                        const k8sContainer = _.get(pod, ['spec', 'containers', 0]);
                        const status = _.get(pod, ['status', 'phase']);

                        const csContainer = _.merge(
                            KubernetesApi.k8sToCSApp(k8sContainer),
                            {status: status === 'Running' ? 
                                'loaded' : 'unloaded'});

                        return [nodeName, csContainer]
                    }),
                    _.partial(_.reduce, _, (result, nodeContainer) => {
                        const [nodeName, container] = nodeContainer;
                        const containersForNode = _.get(result, nodeName, []);
                        return _.set(result, nodeName, 
                            _.concat(containersForNode, container))
                    }, {}))(k8sPods);

                console.log(JSON.stringify(containersByNode));

                const csFollowers = _.map(k8sNodes, _.flow(
                    KubernetesApi.k8sToCSNode,
                    (csNode) => _.merge(csNode, {
                        'metadata': {
                            'containership': {
                                'version': "-1"
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

                /*
                this.getApplications((applications) => {
                    console.log(JSON.stringify(applications));
                });
                */

                const csLeader = {
                    id:os.hostname(),
                    'metadata': {
                        'containership': {
                            'version': "-1"
                        }
                    },
                    praetor: {
                        leader: true,
                        leader_eligible: true,
                        start_time: Date.now()
                    },
                    mode: 'leader',
                    address: {
                        private: "0.0.0.0",
                        public: "0.0.0.0"
                    }, 
                    containers: []
                }

                cb(_.keyBy(_.concat(csLeader, csFollowers), "id"));
            }));
        }));
    }

    /*
    getContainers(cb) {
        request(`${K8S_API_URL}/api/v1/namespaces/default/pods`, util.ifSuccessfulResponse((data) => {
            const pods = data.items;

            console.log(JSON.stringify(pods));


            //We're running one container/pod so grab the first result.
            //const container = KubernetesApi.k8sToCSApp(_.get(data, ['items', 0, 'spec', 'containers', 0]));

            //console.log(container);

            //console.log("have container: " + JSON.stringify(container));
            //const containerStatuses = _.get(data, ['items', 0, 'spec', 'status', 'containerStatuses'], []);
            
        }));
    }
    */
        
}

var add = _.identity;

KubernetesApi.CS_TO_K8S_APPLICATION_MAPPING = {
    "id": [["name"]],
    "privileged": [["securityContext", "privileged"]],
    "image": [["image"]],
    "cpus": [["resources", "limits", "cpu"]],
    "memory": [["resources", "limits", "memory"]],
    "command": [["command"]],
    'privileged': [['securityContext', 'privileged']],
    "respawn": [['restartPolicy']],
    "host_port": [['ports', 0]],
    "container_port": [['ports', 0]],
    "network_mode": [['hostNetwork']]
};

KubernetesApi.CS_TO_K8S_HASH_LOOKUP = {}

KubernetesApi.CS_TO_K8S_NODE_MAPPING = {};
add = _.partial(buildFromLookup, KubernetesApi.CS_TO_K8S_NODE_MAPPING);
add(['id'],         ['metadata', 'name']);
add(['host_name'],  ['metadata', 'uid']);
add(['last_sync'],  ['status', 'conditions', 2, 'lastHeartbeatTime']);
add(['state'],      ['status', 'conditions', 2, 'status']);
add(['start_time'], ['metadata', 'creationTimestamp']);
add(['address', 'public'], ['status', 'addresses', 0]);
add(['address', 'private'], ['status', 'addresses', 1]);

//add(['securityContext', 'privileged'], (v) => v ? v === 'true' : false);
//add(['restartPolicy'], (v) => v ? 'Always': 'Never'); 
//add(['hostNetwork'], (v) => v ? 'host': 'bridge'); 

KubernetesApi.CS_TO_K8S_CONVERSIONS = {
    "memory": (v) => v ? `${v}M`: null,
    "command": (v) => v ? _.split(v, " ") : [],
    'privileged': (v) => v === 'true',
    'host_port': (v) => ({'hostPort': v}),
    'container_port': (v) => ({'containerPort': v}),
    'network_mode': (v) => v ? true : false
};

KubernetesApi.K8S_TO_CS_CONVERSIONS = {};
add = _.partial(buildFromLookup, KubernetesApi.K8S_TO_CS_CONVERSIONS);
add(['memory'],  (v) => v ? parseInt(_.replace(v, ('M', ''))) : null);
add(['command'], (v) => v ? v.join(" ") : null)
add(['cpus'],    (v) => v ? _.flow(
    _.partialRight(_.replace, 'm', ''),
    parseInt,
    (v) => v / 1000)(v) : null);
add(['state'], (v) => v ? _.get({"True": "operational"}, v, "unknown-state") : null);
add(['address', 'public'], (v) => v ? v.address : null);
add(['address', 'private'], (v) => v ? v.address : null);
add(['host_port'], (v) => v ? v.hostPort : null);
add(['container_port'], (v) => v ? v.containerPort : null);
add(['respawn'], (v) => v ? v === 'Always' : false);
add(['network_mode'], (v) => v ? 'host' : 'bridge');

/*
add(['ports'], _.cond([
    [(v, csPath) => { console.log(csPath); return false}, _.identity],
    [(v, csPath) => v && csPath == 'host_port', (v) => v.hostPort],
    [(v, csPath) => v && csPath == 'container_port', (v) => v.containerPort]
]));
*/
    
module.exports = KubernetesApi;

