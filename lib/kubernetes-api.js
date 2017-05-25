const _ = require('lodash');
const os = require('os');
const request = require('request');
const encode = require('hashcode').hashCode().value;

const util = require('./util');
const ApiImplementation  = require('@jeremykross/containership.abstraction.api');

const DEFAULT_K8S_API_URL = "http://localhost:8080";
const DEFAULT_ETCD_API_URL = 'http://localhost:2379';

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
                name: `${_.join(_.take(strEncode(appName), 15), "")}-service` //Limit to 24 chars
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

    static k8sToCSApp(k8sApp) {
        return _.merge({
            respawn: false,
            privileged: false,
            network_mode: 'bridge'
        },
            _.reduce(KubernetesApi.CS_TO_K8S_APPLICATION_MAPPING, (csApp, k8sPaths, csKey) => {

                //FIX ME!
                const csHashKey = strEncode([csKey]);
                const conversionFn = KubernetesApi.K8S_TO_CS_CONVERSIONS[csHashKey];

                const baseValues = _.map(k8sPaths, (k8sPath) => _.get(k8sApp, k8sPath));
                
                const v = conversionFn && baseValues ? 
                    conversionFn.apply(null, _.flatten(_.zip(baseValues, k8sPaths))) :
                    _.first(baseValues);  //Retrofitted to use all the vals.

                return !_.isNil(v) ? _.set(csApp, csKey, v): csApp;
            }, {}));
    }

    static csToK8SApp(csApp) {
        const name = csApp.id;
        const count = csApp.count || 0;

        return _.reduce(KubernetesApi.CS_TO_K8S_APPLICATION_MAPPING, (k8sApp, k8sPaths, csKey) => {
            const conversionFn = KubernetesApi.CS_TO_K8S_CONVERSIONS[csKey] || _.identity;
            const v = csApp[csKey];

            return v ? 
                _.merge(k8sApp, 
                    _.reduce(k8sPaths, (setK8SPaths, path) => {
                        const fullPath = _.concat(['spec', 'template'], path);

                        const currentValue = _.get(setK8SPaths, fullPath);
                        return _.merge(setK8SPaths, 
                            _.set({}, fullPath, conversionFn(v, path)));
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
            const value = conversionFn ? conversionFn(_.get(k8sNode, k8sPath), k8sPath) : _.get(k8sNode, k8sPath);
            return value ? _.set(csNode, csPath, value) : csNode;
        }, {});
    }

    getClusterState() {
        // TODO
    }

    deleteCluster() {
        // TODO
    }

    enforceAllConstraints() {
        this.getHosts((host) => {
            this.getApplications((apps) => {
                _.each(apps, (app) => {
                    this.enforceConstraints(host, app);
                });
            });
        })
    }

    enforceConstraints(hosts, app) {
        const constraints = _.get(app, ["tags", "constraints"], {});

        const perHost = parseInt(_.get(constraints, ["per_host"], 0));
        const min = parseInt(_.get(constraints, ["min"], 0));
        const max = parseInt(_.get(constraints, ["max"], Number.MAX_SAFE_INTEGER));
        
        const running = _.size(_.get(app, "containers", []));

        const followers = _.filter(_.values(hosts), (h) => h.mode === 'follower');
        const followerCount = _.size(followers);
        const neededReplicas = perHost > 0 ? followerCount * perHost : running;

        console.log("Needed replicas: " + neededReplicas);
        console.log("Follower count: " + followerCount);

        const newCount = 
            neededReplicas > max ? max :
            neededReplicas < min ? min :
            neededReplicas;

        console.log("New count: " + newCount);

        if(newCount != running) {
            console.log("Updating");
            this.updateApplication(app.id, {count: newCount}, 
                (x) => console.log(JSON.stringify(x)));
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
                        _.partial(_.get, _, ['spec', 'template']),
                        KubernetesApi.k8sToCSApp,
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

        console.log("Creating service!");

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
            const newApp = _.has(csAppDesc, "tags") && _.isEmpty(csAppDesc.tags) ? 
                _.merge(csAppDesc, initializedConstraints) : csAppDesc;
            

            console.log("Standing app: " + JSON.stringify(existingApp));
            console.log("Update: " + JSON.stringify(newApp));

            //A hack to fix the stange tags handling on UI.
            const k8sDesc = KubernetesApi.csToK8SApp(_.merge(existingApp, newApp));

            console.log("New rc: " + JSON.stringify(k8sDesc));

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
        ])(csAppDesc.container_port, csAppDesc.host_port, csAppDesc.random_host_port || false);
            
        const containerPort = csAppDesc.container_port ? csAppDesc.container_port : hostPort;

        /*dispatch({
            type: EVT_MARK_PORT_USED,
            port: hostPort
        });*/

        const k8sAppDesc = KubernetesApi.csToK8SApp(_.merge(csAppDesc, {
            host_port: hostPort, 
            container_port: containerPort,
            env_vars: { CS_ORCHESTRATOR: "kubernetes" }
        }));

        console.log("Creating k8s application: " + JSON.stringify(k8sAppDesc));

        request({ 
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers`,
            method: 'POST',
            body: k8sAppDesc,
            json: true
        }, (err, res, body) => { 

            console.log("Back from create: " + err + " " + res + " " + JSON.stringify(body));

            //Create a service for this application.
            if(!err) {

                const ports = _.get(k8sAppDesc,['spec', 'template', 'spec', 'containers', 0, 'ports']);

                if(_.size(ports) > 0) {
                    this.createService(
                        k8sAppDesc, 
                        ports, 
                        [],
                        (err, res, body) => { 
                            console.log("Back from create service: " + " error: " + err + JSON.stringify(body));
                            return cb(null, res.statusCode, body); 
                        }
                    );
                } else {
                    cb(null, res.statusCode, body);
                }

            } else {
                console.log(JSON.stringify(err));
                console.log(res);
                cb(err, body);
            }
        });
    }

    deleteApplication(name, cb) {
        request({
            uri: `${this.k8sApiUrl}/api/v1/namespaces/default/replicationcontrollers/${name}`,
            method: 'DELETE'
        }, (err, res, body) => {
            cb(res.statusCode, body);
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
                                    return _.get(node, ['metadata', 'labels', 'cs-node-id'], "NOT FOUND!");
                                })(k8sNodes);

                        const k8sContainer = pod;
                        const status = _.get(pod, ['status', 'phase']);
                        const containerID = _.replace(_.get(pod, ['status', 'containerStatuses', 0, 'containerID']), "docker://", "");

                        const csContainer = _.merge(
                            KubernetesApi.k8sToCSApp(k8sContainer),
                            { 
                                status: status === 'Running' ? 'loaded' : 'unloaded',
                                container_id: _.replace(containerID, "docker://", ""),
                                id: containerID
                            });

                        return [csNodeName, csContainer]
                    }),
                    _.partial(_.reduce, _, (result, nodeContainer) => {
                        const [nodeName, container] = nodeContainer;
                        const containersForNode = _.get(result, nodeName, []);
                        return _.set(result, nodeName, _.concat(containersForNode, container))
                    }, {}))(k8sPods);

                const csFollowers = _.map(k8sNodes, _.flow(
                    KubernetesApi.k8sToCSNode,
                    (csNode) => _.merge(csNode, {
                        'metadata': {
                            'containership': {
                                'version': "0.0.0"
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

                /*
                this.getApplications((applications) => {
                    console.log(JSON.stringify(applications));
                });
                */

                const csLeader = {
                    'id': process.argv[6] || os.hostname(),

                    'metadata': {
                        'containership': {
                            'version': "0.0.0"
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
                }

                cb(_.keyBy(_.concat(csLeader, csFollowers), "id"));
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

    getDistributedKey(key, cb) {
        const req = {
            uri: `${this.etcdApiUrl}/v2/keys/${key}`,
            method: 'GET', 
            json: true
        }

        request(req, (err, resp, body) => {
            if(err) {
                cb(err);
            } else {
                cb(null, util.safeParse(_.get(body, ['node', 'value'])));
            }
        });
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

KubernetesApi.CS_TO_K8S_HASH_LOOKUP = {}

/*
KubernetesApi.CS_TO_K8S_APPLICATION_MAPPING = {};
add = _.partial(buildFromLookup, KubernetesApi.CS_TO_K8S_APPLICATION_MAPPING);
*/

KubernetesApi.CS_TO_K8S_APPLICATION_MAPPING = {
    "id": [["spec", "containers", 0, "name"]],
    "name": [["spec", "containers", 0, "name"], ['metadata', 'labels', 'app']],
    "env_vars": [["spec", "containers", 0, "env"]],
    "privileged": [["spec", "containers", 0, "securityContext", "privileged"]],
    "image": [["spec", "containers", 0, "image"]],
    "cpus": [["spec", "containers", 0, "resources", "limits", "cpu"]],
    "memory": [["spec", "containers", 0, "resources", "limits", "memory"]],
    "command": [["spec", "containers", 0, "command"]],
    'privileged': [["spec","containers", 0, 'securityContext', 'privileged']],
    "respawn": [["spec","containers", 0, 'restartPolicy']],
    "host_port": [["spec","containers", 0, 'ports', 0]],
    "container_port": [["spec","containers", 0, 'ports', 0]],
    "volumes": [["spec","containers", 0, "volumeMounts"], ['spec', 'volumes']],
    "network_mode": [["spec",'hostNetwork']],
    'tags': [['metadata', 'labels']]
};

/*
add(["id"], [["containers", 0, "name"]]);
add(["env_vars"], [["containers", 0, "env"]]);
add(["privileged"], [["containers", 0, "securityContext", "privileged"]]);
add(["image"], [["containers", 0, "image"]]);
add(["cpus"], [["containers", 0, "resources", "limits", "cpu"]]);
add(["memory"], [["containers", 0, "resources", "limits", "memory"]])
add(["command"], [["containers", 0, "command"]])
add(['privileged'], [["containers", 0, 'securityContext', 'privileged']]);
add(["respawn"], [["containers", 0, 'restartPolicy']]);
add(["host_port"], [["containers", 0, 'ports', 0]]);
add(["container_port"], [["containers", 0, 'ports', 0]]);
add(["volumes"], [["containers", 0, "volumeMounts"], ['volumes']]);
add(["network_mode"], [['hostNetwork']]);
*/

KubernetesApi.CS_TO_K8S_NODE_MAPPING = {};
add = _.partial(buildFromLookup, KubernetesApi.CS_TO_K8S_NODE_MAPPING);
add(['id'],         ['metadata', 'labels', 'cs-node-id']);
add(['host_name'],  ['metadata', 'name']);
add(['last_sync'],  ['status', 'conditions', 2, 'lastHeartbeatTime']);
add(['state'],      ['status', 'conditions', 2, 'status']);
add(['start_time'], ['metadata', 'creationTimestamp']);
add(['address', 'public'], ['status', 'addresses', 0]);
add(['address', 'private'], ['status', 'addresses', 1]);
add(['cpus'], ['status', 'capacity', 'cpu']);
add(['memory'], ['status', 'capacity', 'memory']);


KubernetesApi.CS_TO_K8S_CONVERSIONS = {
    "command": (v) => v ? _.split(v, " ") : [],
    'privileged': (v) => v === 'true',
    'host_port': (v) => ({'hostPort': v}),
    'container_port': (v) => ({'containerPort': v}),
    'network_mode': (v) => v === 'host' ? true : false,

    'env_vars': (v) => v ? _.map(v, (value, name) => {
        return { name: name, value: String(value) };
    }) : [],

    'cpus': (v, destinationPath) => {

        if(!v) return null;

        if(_.includes(destinationPath, 'capacity')) {
            return v;
        } else {
            return `${v * 1000}m`
        }


    },

    "memory": (v, destinationPath) => {

        if(!v) return null;

        if(_.includes(destinationPath, 'capacity')) {
            _.flow(
                _.partial(_.replace, _, 'M', ''),
                _.partial(_.parseInt),
                (v) => v * 1000,
                (v) => `${v}Ki`)(v);
        } else {
            return `${v}M`;
        }

    },

    //Look here, need proper volume mounting.
    'volumes': (volumes, destinationPath) => {

        const pathToName = (p) => p !== "/" ? _.replace(p, new RegExp("/", 'g'), "") : "root";

        return _.includes(destinationPath, 'containers') ?
            _.map(volumes, (v) => ({
                name: pathToName(v.host),
                mountPath: v.container
            })) :
            _.map(volumes, (v) => ({
                name: pathToName(v.host),
                hostPath: {
                    path: v.host
                }
            }))
    },

    'tags': (v) =>  _.flow(
        _.partial(_.get, _, ["constraints"], {per_host: 0}),
        _.partial(_.mapKeys, _, (v, k) => `tags.constraints.${k}`),
        _.partial(_.mapValues, _, (v, k) => `${v}`)
    )(v)
};

KubernetesApi.K8S_TO_CS_CONVERSIONS = {};
add = _.partial(buildFromLookup, KubernetesApi.K8S_TO_CS_CONVERSIONS);
add(['command'], (v) => v ? v.join(" ") : null)
add(['state'], (v) => v ? _.get({"True": "operational"}, v, "unknown-state") : null);
add(['address', 'public'], (v) => v ? v.address : null);
add(['address', 'private'], (v) => v ? v.address : null);
add(['host_port'], (v) => v ? v.hostPort : null);
add(['container_port'], (v) => v ? v.containerPort : null);
add(['respawn'], (v) => v ? v === 'Always' : false);
add(['network_mode'], (v) => v ? 'host' : 'bridge');

add(['env_vars'], (envVars) => {
    return _.merge.apply(null,
        _.map(envVars, (v) => {
            return _.set({}, v.name, v.value);
        }), {});
});

add(['cpus'], (v, sourcePath) =>  {

    if(!v) return null;

    //Conversion for host
    if(_.includes(sourcePath, 'capacity')) {
        return v;
    } else {//Conversion for container spec
        return _.flow(
            _.partialRight(_.replace, 'm', ''),
            parseInt,
            (v) => v / 1000)(v)
    }

});

add(['memory'],  (v, sourcePath) => {
    if(!v) return null;

    if(_.includes(sourcePath, 'capacity')) {
        return parseInt(_.replace(v, 'Ki', '')) * 1000;
    } else {
        return parseInt(_.replace(v, ('M', '')));
    }
});

add(['volumes'], (volumeMounts, srcPath0, volumeDests, srcPath1) => {
    const volumes = _.zipWith(volumeMounts, volumeDests, _.merge);
    return _.map(volumes, (v) => ({
        host: v.hostPath.path,
        container: v.mountPath
    }));
});

add(['tags'], (v) =>  _.flow(
    _.partial(_.pickBy, _, (v, k) => _.startsWith(k, "tags")),
    _.partial(_.mapKeys, _, (v, k) => _.replace(k, "tags.", "")),
    _.partial(_.reduce, _, (csTags, v, k) => (_.set(csTags, k, v)), {}) 
)(v));


module.exports = KubernetesApi;

