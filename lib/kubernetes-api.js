'use strict';

const logger = require('./logger');
const util = require('./util');

const _ = require('lodash');
const ApiImplementation = require('@containership/containership.abstraction.api');
const async = require('async');
const constants = require('containership.core.constants');
const CSUtil = require('containership.utils');
const EventEmitter = require('events');
const flatten = require('flat');
const request = require('request');
const Translator = require('@containership/containership.k8s.translator');

const DEFAULT_K8S_API_URL = process.env.K8S_API_URL || 'http://localhost:8080';
const DEFAULT_ETCD_API_URL = process.env.K8S_ETCD_API_URL || 'http://localhost:2379';

class KubernetesApi extends ApiImplementation {

    constructor(k8s_api_ip, k8s_api_port) {
        super();

        this.k8s_api_ip = k8s_api_ip;
        this.k8s_api_port = k8s_api_port;

        this.k8s_api_url = this.k8s_api_ip && this.k8s_api_port ?
            `http://${this.k8s_api_ip}:${this.k8s_api_port}` :
            DEFAULT_K8S_API_URL;

        this.etcd_api_url = this.k8s_api_ip ?
            `http://${this.k8s_api_ip}:2379` :
            DEFAULT_ETCD_API_URL;

        this.defaultErrorHandler = (err) => {
            logger.info(`There was an error in the K8S Api Bridge:`);
            logger.error(err);
        };

        this.middleware = {
            pre_deploy: [
                // clear containership specific environment variables
                (app_desc, cb) => {
                    app_desc.env_vars = _.omitBy(app_desc.env_vars || {}, (value, key) => {
                        return key.indexOf('CS_') === 0;
                    });

                    return cb(null, app_desc);
                },

                // inject some standard CS_ vars
                (app_desc, cb) => {
                    app_desc.env_vars = app_desc.env_vars || {};

                    app_desc.env_vars.CS_ORCHESTRATOR = 'kubernetes';
                    app_desc.env_vars.CS_API_IP = this.k8s_api_ip;

                    return cb(null, app_desc);
                },

                // build service discovery environment variables
                (app_desc, cb) => {
                    app_desc.env_vars = app_desc.env_vars || {};

                    this._getClusterId((err) => { /* ,cluster_id ) => { */
                        if(err) {
                            return cb(err);
                        }

                        this.getApplications((err, apps) => {
                            if(err) {
                                return cb(err);
                            }

                            _.forEach(apps, (app) => {
                                const sanitized_app_id = app.id.replace(/-/g, '_');
                                const discovery_port_env_var_name = `CS_DISCOVERY_PORT_${sanitized_app_id.toUpperCase()}`;

                                if(app.discovery_port) {
                                    app_desc.env_vars[discovery_port_env_var_name] = app.discovery_port.toString();
                                }

                                const address_env_var_name = `CS_ADDRESS_${sanitized_app_id.toUpperCase()}`;
                                // set to kube-dns service address as described in: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/
                                app_desc.env_vars[address_env_var_name] = `${app.id}.default`;
                            });

                            return cb(null, app_desc);
                        });
                    });
                }
            ]
        };

        this.ifSuccessfulResponse =
            CSUtil.ifAcceptableResponseFn(

                // On Error.
                (err, res, options) => {

                    const errHandler = _.get(options, ['errorHandler']);
                    const failedMethod = _.get(options, ['method'], 'unknown-method');

                    logger.info(`Failed making call to K8S API in ${failedMethod} // err: ${err}, res: ${res.statusCode} - ${JSON.stringify(res.body)}.`);

                    if(errHandler) {
                        errHandler(err);
                    }

                },

                //Check acceptance
                (res) => Math.floor(res.statusCode / 200) === 1);

    }

    static k8sServiceDescriptionFor(app_desc, ports) {
        const app_name = _.get(app_desc, 'metadata.name');

        return {
            kind: 'Service',
            metadata: {
                name: app_name
            },
            spec: {
                type: 'NodePort',
                ports: _.map(ports, (port) => {
                    return {
                        port: port.containerPort
                    };
                }),
                selector: {
                    app: app_name
                }
            }
        };
    }

    _getClusterId(cb) {
        return this.getDistributedKey(constants.myriad.CLUSTER_ID, cb);
    }

    //private, k8s services are analogous to cs service-discovery.
    createService(k8s_app_desc, k8s_ports, cb) {
        const service_body = KubernetesApi.k8sServiceDescriptionFor(k8s_app_desc, k8s_ports);
        const app_name = _.get(k8s_app_desc, 'metadata.name');

        const req = {
            uri: `${this.k8s_api_url}/api/v1/namespaces/default/services`,
            method: 'POST',
            json: true,
            body: service_body
        };

        return request(req, (err, res) => {
            if(err || res.statusCode !== 201) {
                return cb(Error(`Could not create service: ${service_body.metadata.name} for application: ${app_name}`));
            }

            return cb();
        });
    }

    //private, k8s services are analogous to cs service-discovery.
    updateService(k8s_app_desc, k8s_ports, cb) {
        const service_body = KubernetesApi.k8sServiceDescriptionFor(k8s_app_desc, k8s_ports);
        const app_name = _.get(k8s_app_desc, 'metadata.name');

        const req = {
            uri: `${this.k8s_api_url}/api/v1/namespaces/default/services/${app_name}`,
            method: 'PATCH',
            json: true,
            body: service_body,
            headers: {
                'Content-Type': 'application/merge-patch+json'
            }
        };

        return request(req, (err, res) => {
            if(err || res.statusCode !== 200) {
                return cb(Error(`Could not update service: ${service_body.metadata.name} for application: ${app_name}`));
            }

            return cb();
        });
    }

    deleteService(app_name, cb) {
        const req = {
            uri: `${this.k8s_api_url}/api/v1/namespaces/default/services/${app_name}`,
            method: 'DELETE'
        };

        return request(req, (err, res) => {
            if(err || res.statusCode !== 200) {
                return cb(Error(`Failed to delete service: ${app_name}`));
            }

            return cb();
        });
    }

    _deleteNodeIfUnhealthy(node_id) {
        return this.getHost(node_id, (err, host) => {
            if(err) {
                return logger.error(err);
            }

            if(host.state === 'NotReady') {
                return this.deleteHost(host.id, (err) => {
                    if(err) {
                        return logger.error(err);
                    }

                    return logger.info(`Deleted unresponsive node: ${node_id}`);
                });
            }
        });
    }

    enforceNodeLiveliness() {
        return this.getHosts((err, hosts) => {
            if(err) {
                return logger.error(err);
            }

            return _.forEach(hosts, (host) => {
                if(host.state === 'NotReady') {
                    // give host one minute to resolve itself, otherwise remove node from k8s
                    logger.info(`Host ${host.id} is in a NotReady state. Removing is still unhealthy in 60 seconds...`);
                    setTimeout(this._deleteNodeIfUnhealthy.bind(this, host.id), 1000 * 60);
                }
            });
        });
    }

    enforceAllConstraints() {
        return this.getApplications((err, apps) => {
            if(err) {
                return logger.error(err);
            }

            return _.forEach(apps, (app) => {
                return this.updateApplication(app.id, {}, (err) => {
                    if(err) {
                        return logger.info(`Error updating application when enforcing constraints for ${app.id}: ${err}`);
                    }

                    return logger.info(`Enforced constraints for ${app.id}`);
                });
            });
        });
    }

    getApplications(cb) {
        return async.parallel({
            replication_controllers: (cb) => {
                return request(`${this.k8s_api_url}/api/v1/namespaces/default/replicationcontrollers`,
                    (err, res, body) => {
                        if(err || res.statusCode !== 200) {
                            return cb(Error('Failed to retrieve replication controllers inside getApplications'));
                        }

                        return cb(null, util.safeParse(body));
                    });
            },
            services: (cb) => {
                return request(`${this.k8s_api_url}/api/v1/namespaces/default/services`,
                    (err, res, body) => {
                        if(err || res.statusCode !== 200) {
                            return cb(Error('Failed to retrieve the services inside getApplications'));
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
                return cb(err);
            }

            const replication_controllers = responses.replication_controllers;
            const services = responses.services;

            const k8s_items = replication_controllers.items || [];
            const k8s_services = services.items || [];

            const all_containers = responses.containers;

            const containers_by_app = _.reduce(all_containers, (result, container) => {
                const app_name = container.name;
                const containers = result[app_name] || [];
                result[app_name] = _.concat(containers, container);
                return result;
            }, {});

            const cs_items = _.map(k8s_items, _.flow(
                Translator.csApplicationFromK8SReplicationController,
                (cs_app) => {
                    // Augment the app with the discovery port.
                    const k8s_service_for_app = _.first(_.filter(k8s_services, (service) =>
                        _.get(service, 'spec.selector.app') === cs_app.id));

                    const discovery_port = _.get(k8s_service_for_app, 'spec.ports[0].nodePort');

                    return discovery_port ?
                        _.merge(cs_app, {'discovery_port': discovery_port}) :
                        cs_app;

                },
                (cs_app) => {
                    // filter out containership specific environment variables
                    cs_app.env_vars = _.omitBy(cs_app.env_vars, (value, key) => {
                        // check if port was manually defined by the end user
                        if(key === 'PORT' && value === cs_app.container_port) {
                            return true;
                        // strip all keys with 'CS_' prefix
                        } else if(key.indexOf('CS_') === 0) {
                            return true;
                        // all other keys are valid
                        } else {
                            return false;
                        }
                    });

                    return cs_app;
                },
                (cs_app) => {
                    cs_app.containers = containers_by_app[cs_app.id] || [];
                    return cs_app;
                }));

            const cs_apps =
                _.merge.apply(null, _.map(cs_items, (item) => {
                    return _.set({}, item.id, _.defaults(item, {
                        env_vars: {},
                        tags: {},
                        volumes: []
                    }));
                }));

            return cb(null, cs_apps);
        });
    }

    getApplication(application, cb) {
        return this.getApplications((err, apps) => {
            if(err) {
                return cb(err);
            }

            const app = _.first(_.filter(apps, (v, k) => k === application));

            if(!app) {
                return cb({
                    error: Error(`The requested application does not exist: ${application}`),
                    statusCode: 404
                });
            }

            return cb(null, app);
        });
    }

    getContainers(cb) {
        return request(`${this.k8s_api_url}/api/v1/pods`, (err, res, body) => {
            if(err || res.statusCode !== 200) {
                return cb(Error('Failed to retrive k8s pods inside getContainers call'));
            }

            const pods = util.safeParse(body).items;

            const containers = _.map(pods, pod => {
                const node_name = _.get(pod, 'spec.nodeName');
                const k8s_container = pod.spec;

                const status = Translator.csStatusFromK8SStatus(pod);
                const start_time = Date.parse(_.get(pod, 'status.startTime'));
                const internal_container_ip = _.get(pod, 'status.podIP') || _.get(pod, 'status.hostIP');
                const container_id = _.get(pod, 'metadata.uid');
                const app_name = _.get(pod, 'metadata.labels.app');
                const name = _.get(pod, 'status.containerStatuses[0].name');
                const image = _.get(pod, 'status.containerStatuses[0].image');

                const cs_container = _.merge(
                    Translator.csApplicationFromK8SPodSpec(k8s_container),
                    {
                        status: status,
                        container_id: container_id,
                        id: container_id,
                        name: name,
                        image: image,
                        host: node_name,
                        internal_container_ip: internal_container_ip,
                        start_time: start_time,
                        application: app_name
                    }
                );

                return cs_container;
            });

            return cb(null, containers);
        });
    }

    createContainers(app_id, container_config, cb) {
        return this.getApplications((err, apps) => {
            if(err) {
                return cb(err);
            }

            const app = _.first(_.filter(apps, (v, k) => k === app_id));

            if(!app) {
                return cb({
                    error: Error(`Couldn't find application with id ${app_id} when in KubernetesApi::createContainer!`),
                    statusCode: 400
                });
            }

            const container_count = _.size(app.containers);
            const new_count = container_count + parseInt(container_config.count);

            this.updateApplication(app_id, _.merge(container_config, {
                count: new_count
            }), cb);
        });
    }

    deleteContainer(app_id, container_id, cb) {
        return this.getApplications((err, apps) => {
            if(err) {
                return cb(err);
            }

            const app = _.first(_.filter(apps, (v, k) => k === app_id));

            if(!app) {
                return cb({
                    error: Error(`Couldn't find application with id ${app_id} when in KubernetesApi::deleteContainer!`),
                    statusCode: 400
                });
            }

            const container_count = _.size(app.containers);
            const new_count = container_count - 1;

            return this.updateApplication(app_id, { count: new_count }, cb);
        });
    }

    addPreDeployMiddleware(middlewareFn) {
        this.middleware.pre_deploy.push(middlewareFn);
    }

    _executePreDeployMiddleware(app_desc, cb) {
        const middleware = _.cloneDeep(this.middleware.pre_deploy);

        // pass base app_desc to first middleware function
        middleware.unshift((cb) => {
            return cb(null, app_desc);
        });

        async.waterfall(middleware, cb);
    }

    _redeployContainers(app_id, cb) {
        const getRunningContainerCount = (runningContainerCb) => {
            this.getApplication(app_id, (err, app) => {
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
            (current_container_count, cb) => {
                this.scaleDownContainers(app_id, current_container_count, (err) => {
                    if(err) {
                        return cb(err);
                    }

                    return cb(null, current_container_count);
                });
            },

            // wait until number of containers has reached 0
            (starting_container_count, cb) => {
                async.retry({ times: 12, interval: 5000 }, (retryCb) => {
                    getRunningContainerCount((err, num_containers) => {
                        if(err) {
                            return retryCb(err);
                        } else if(num_containers !== 0) {
                            return retryCb(new Error(`${num_containers} containers still running ...`));
                        } else {
                            return retryCb(null, starting_container_count);
                        }
                    });
                }, cb);
            },

            (desired_containers, cb) => {
                this.scaleUpContainers(app_id, desired_containers, cb);
            }
        ], (err, response) => {
            if(err) {
                return cb(err);
            }

            return cb(null, response);
        });
    }

    _applyConstraintsToApplication(app, cb) {
        const app_desc = _.cloneDeep(app);

        this.getHosts((err, hosts) => {
            if(err) {
                return cb(err);
            }

            // short circuit if application was forcefully scaled to 0 containers
            if(app_desc.count === 0) {
                return cb(null, app_desc);
            }

            const constraints = _.get(app_desc, 'tags.constraints', {});

            const per_host = parseInt(constraints.per_host || 0);
            const min = parseInt(constraints.min || 0);
            const max = parseInt(constraints.max || Number.MAX_SAFE_INTEGER);

            const running = app_desc.count !== undefined ? app_desc.count : _.size(app_desc.containers);

            const followers = _.filter(_.values(hosts), (h) => h.mode === 'follower' && h.state === 'Ready');
            const follower_count = _.size(followers);
            const needed_replicas = per_host > 0 ? follower_count * per_host : running;

            const desired_count =
                needed_replicas > max ? max :
                    needed_replicas < min ? min :
                        needed_replicas;

            if(desired_count !== running) {
                app_desc.count = desired_count;
            }

            return cb(null, app_desc);
        });
    }

    updateAndReturnApplication(app_id, app_desc_update, cb) {
        return this.updateApplication(app_id, app_desc_update, (err) => {
            if(err) {
                return cb(err);
            }
            // need to get the updated application in containership format
            return this.getApplication(app_id, cb);
        });
    }

    updateApplication(app_id, app_desc_update, cb) {
        return this.getApplication(app_id, (err, existing_app) => {
            if(err) {
                return cb(err);
            }

            let update_service = false;
            if(_.has(app_desc_update, 'container_port')) {
                update_service = true;
            }

            if(_.has(app_desc_update, 'env_vars')) {
                existing_app.env_vars = app_desc_update.env_vars;
            }

            if(_.has(app_desc_update, 'tags')) {
                existing_app.tags = app_desc_update.tags;
            }

            // change the existing apps volumes to only be the updated value so they don't get merged together
            // all volumes that should be mounted should be sent in the request
            if(_.has(app_desc_update, 'volumes')) {
                existing_app.volumes = app_desc_update.volumes;
            }

            // if updating to host mode, need to ensure new app host_port === container_port
            if(app_desc_update.network_mode === 'host') {
                app_desc_update.host_port = app_desc_update.container_port || existing_app.container_port;
            }

            // if existing app is in host mode and we are not updating to bridge mode, and providing a new container_port, we must update host port
            if(existing_app.network_mode === 'host' && !app_desc_update.network_mode !== 'bridge' && app_desc_update.container_port !== undefined) {
                app_desc_update.host_port = app_desc_update.container_port;
            }

            // if we are switching to bridge mode, set the host_port to null so it is managed by kubernetes
            if(app_desc_update.network_mode === 'bridge') {
                app_desc_update.host_port = null;
            }

            let properties = _.keys(flatten(app_desc_update));

            const replication_controller_needs_updated = !_.isEmpty(properties);

            _.remove(properties, (property) => {
                return property.indexOf('tags.constraints') === 0 || property === 'count';
            });

            const containers_need_redeployed = !_.isEmpty(properties);

            const updateReplicationController = (cb) => {
                _.merge(existing_app, app_desc_update);

                // count is not being explicitly set to 0, so if the app count is already zero,
                // set to null so that applyConstraints does not short-circuit setting desiredReplicas
                if(_.isUndefined(app_desc_update.count) && existing_app.count === 0) {
                    existing_app.count = null;
                }

                this._applyConstraintsToApplication(existing_app, (err, app_desc) => {
                    if(err) {
                        return cb(err);
                    }

                    // set back to 0 to continue normal execution flow and so it matches
                    // in equality comparison below
                    if(existing_app.count === null) {
                        existing_app.count = 0;
                    }

                    this._executePreDeployMiddleware(app_desc, (err, app_desc) => {
                        if(err) {
                            return cb(err);
                        }

                        if(!replication_controller_needs_updated && _.isEqual(existing_app, app_desc)) {
                            return cb();
                        } else {
                            existing_app = app_desc;
                        }

                        // Pull out the tags in the object root and flatten
                        const tags = _.flow(
                            _.partial(_.get, _, 'tags'),
                            _.partial(_.omit, _, 'metadata', 'constraints'),
                            (t) => !_.isEmpty(t) ? { tags: t } : {},
                            flatten)(existing_app);

                        // Apply them to the kubernetes nodeSelector.
                        const k8s_app_desc = _.flow(
                            Translator.csApplicationToK8SReplicationController,
                            _.partial(_.set, _, 'spec.template.spec.nodeSelector', tags))(existing_app);

                        return request({
                            uri: `${this.k8s_api_url}/api/v1/namespaces/default/replicationcontrollers/${app_id}`,
                            method: 'PUT',
                            body: k8s_app_desc,
                            json: true,
                            headers: {
                                'Content-Type': 'application/json'
                            }
                        }, (err, res, body) => {
                            if(err || res.statusCode !== 200) {
                                return cb(err || body);
                            }

                            if(update_service) {
                                const ports = _.get(body, 'spec.template.spec.containers[0].ports', []);

                                if(_.size(ports) === 0) {
                                    return cb(null, body);
                                }

                                return this.updateService(k8s_app_desc, ports, cb);
                            }

                            return cb(null, body);
                        });
                    });
                });
            };

            return updateReplicationController((err) => {
                if(err) {
                    return cb(err);
                }

                if(containers_need_redeployed) {
                    this._redeployContainers(app_id, (err) => {
                        if(err) {
                            logger.info(`There was an error redeploying containers.`);
                            logger.error(err);
                        }
                    });
                }

                return cb();
            });
        });
    }

    createApplication(app_desc, cb) {
        // only use provided host port if it is in `host` network mode
        const host_port = (app_desc.network_mode && app_desc.network_mode === 'host') ? app_desc.host_port : null;
        app_desc.container_port = host_port ? host_port : (app_desc.container_port ? app_desc.container_port : _.random(11024, 22047));

        // temporary shim for managed volumes in containership. builds a predefined
        // host volume based on path and a uuid. note this will not be unique if
        // more than one container is running on the same host currently
        app_desc.volumes = _.map(app_desc.volumes || [], (vol) => {
            if(_.isUndefined(vol.host)) {
                vol.host = `/opt/containership/codexd/${app_desc.id}`;
            }

            return vol;
        });

        // set the number of containers to 0 for the application unless explicitly set in request
        app_desc.count = app_desc.containers ? app_desc.containers.length : 0;

        this._executePreDeployMiddleware(app_desc, (err, app_desc) => {
            if(err) {
                return cb(err);
            }

            const k8s_app_desc = Translator.csApplicationToK8SReplicationController(app_desc);

            return request({
                uri: `${this.k8s_api_url}/api/v1/namespaces/default/replicationcontrollers`,
                method: 'POST',
                body: k8s_app_desc,
                json: true
            }, (err, res, body) => {
                if(err || res.statusCode !== 201) {
                    return cb({
                        error: Error(`Could not create replication controller for ${app_desc.id}`),
                        statusCode: res.statusCode >= 400 && res.statusCode <= 499 ? 400 : 500
                    });
                }

                const ports = _.get(k8s_app_desc, 'spec.template.spec.containers[0].ports', []);

                if(_.size(ports) === 0) {
                    return cb(null, body);
                }

                return this.createService(k8s_app_desc, ports, (err) => {
                    if(err) {
                        return cb(err);
                    }

                    return cb(null, body);
                });
            });
        });
    }

    createApplications(app_descriptions, options, cb) {
        if(typeof options === 'function') {
            cb = options;
            options = {};
        }

        return async.waterfall([
            // get applications
            (get_cb) => this.getApplications(get_cb),
            (existing_apps, remove_cb) => {
                if(options.remove_existing !== true) {
                    return setImmediate(() => {
                        return remove_cb(null, existing_apps);
                    });
                }

                return async.eachLimit(existing_apps, 10, (app, cb) => {
                    return this.deleteApplication(app.id, cb);
                }, (err) => {
                    return remove_cb(err, []);
                });
            },
            (existing_apps, add_cb) => {
                return async.each(app_descriptions, (app, createCallback) => {
                    if(_.find(existing_apps, { id: app.id })) {
                        return setImmediate(createCallback);
                    }

                    return this.createApplication(app, createCallback);
                }, add_cb);
            }
        ], (err) => {
            if(err) {
                return cb(err);
            }

            return cb();
        });
    }

    deleteApplication(name, cb) {
        return this.updateApplication(name, { tags: { constraints: {} }, count: 0 }, (err) => {
            if(err) {
                return cb(err);
            }

            const self = this;

            function safeDeleteRC(delete_cb) {
                self.getApplication(name, (err, application) => {
                    if(err) {
                        return delete_cb(err);
                    }

                    let pod_count = 0;
                    _.forEach(application.containers, (container) => {
                        if(container.status !== 'terminating' && container.status !== 'terminated') {
                            pod_count++;
                        }
                    });

                    logger.info('Surviving pod count: ' + pod_count);

                    if(pod_count > 0) {
                        return delete_cb(Error(`There are still pods: ${pod_count}...waiting 4 seconds for pods to delete`));
                    }

                    logger.info('Making final delete.');

                    return async.parallel([
                        (cb) => {
                            return request({
                                uri: `${self.k8s_api_url}/api/v1/namespaces/default/replicationcontrollers/${name}`,
                                method: 'DELETE'
                            }, (err, res) => {
                                if(err || res.statusCode !== 200) {
                                    return cb(Error(`Failed to delete replication controller: ${name}`));
                                }

                                return cb();
                            });
                        },
                        (cb) => {
                            self.deleteService(name, cb);
                        }
                    ], delete_cb);
                });
            }

            return async.retry({ times: 12, interval: 5000 }, safeDeleteRC, cb);
        });
    }

    getHost(host, cb) {
        return this.getHosts((err, hosts) => {
            if(err) {
                return cb(err);
            }

            if(!hosts[host]) {
                return cb({
                    error: Error(`The requested host does not exist: ${host}`),
                    statusCode: 400
                });
            }

            return cb(null, hosts[host]);
        });
    }

    updateHost(host_id, tags, cb) {
        if(!tags) {
            return cb(Error('You must provide tags to update on the host'));
        }

        return this.getHost(host_id, (err, host) => {
            if(err) {
                return cb({
                    error: err,
                    statusCode: 400
                });
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
                uri: `${this.k8s_api_url}/api/v1/nodes/${host_id}`,
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
                    return cb({
                        err: err,
                        statusCode: res.statusCode
                    });
                }

                return cb(null, body);
            });
        });
    }

    deleteHost(host_id, cb) {
        async.parallel({

            // Grab the hosts.
            hosts: (cb) => {
                return this.getHosts(cb);
            },

            // And etcd members.
            etcd_members: (cb) => {
                request({
                    uri: `${this.etcd_api_url}/v2/members`,
                    method: 'GET',
                    json: true
                }, (err, res) => {
                    if(err) {
                        logger.error(`Error determining etcd host for k8s deletion on ${host_id}: ${err}.`);
                        return cb(err);
                    }

                    return cb(null, _.get(res, ['body', 'members'], []));
                });
            }
        }, (err, result) => {

            if(err) {
                logger.error(`Couldn't fetch requisite data in deleteHost(): ${err}`);
                return cb(err);
            }

            const { hosts, etcd_members } = result;

            async.series([

                (cb) => {
                    const host = hosts[host_id];
                    const host_url = _.get(host, 'address.private', null);

                    //Find the etcd id associated with the kuber host
                    const etcd_member_id = _.flow(
                        _.partial(_.filter, _, (member) => {
                            const client_urls = _.get(member, 'clientURLs', []);

                            if(member.name) {
                                return member.name === host_id;
                            }

                            return _.some(client_urls, (client_url) => {
                                return _.includes(client_url, host_url);
                            });
                        }),
                        _.head,
                        _.partial(_.get, _, 'id')
                    )(etcd_members);

                    if(!etcd_member_id) {
                        logger.error('Couldn\'t find associated etcd member id.');
                        // continue to delete the k8s node_id. etcd_manager will automatically
                        // clean up hanging etcd_member if it exists
                        return cb();
                    }

                    // Request it's deletion
                    request({
                        uri: `${this.etcd_api_url}/v2/members/${etcd_member_id}`,
                        method: 'DELETE'
                    }, (err, res) => {
                        if(err || Math.floor(res.statusCode / 200) !== 1) {
                            logger.error(`Error deleting etcd host ${etcd_member_id}: ${err}`);
                            return cb(err || res);
                        }

                        return cb();
                    });
                },
                (cb) => {
                    //If etcd deletion was successful, go ahead and remove the kuber node.
                    return request({
                        uri: `${this.k8s_api_url}/api/v1/nodes/${host_id}`,
                        method: 'DELETE'
                    }, (err, res) => {
                        if(err || Math.floor(res.statusCode / 200) !== 1) {
                            logger.error(`Error deleting k8s host ${host_id}.`);
                            return cb(err || res);
                        }

                        return cb();
                    });
                }
            ], (err) => {
                if(err) {
                    logger.error(`Caught an error during final callback during deleteHost: ${err}.`);
                    return cb(err);
                }

                this.enforceAllConstraints();
                return cb();
            });

        });

    }

    getHosts(cb) {
        return async.parallel({
            nodes: (cb) => {
                return request(`${this.k8s_api_url}/api/v1/nodes`,
                    (err, res, body) => {
                        if(err || res.statusCode !== 200) {
                            return cb(Error('Failed to retrive k8s nodes inside getHosts call'));
                        }

                        return cb(null, util.safeParse(body));
                    });
            },
            services: (cb) => {
                return request(`${this.k8s_api_url}/api/v1/services`,
                    (err, res, body) => {
                        if(err || res.statusCode !== 200) {
                            return cb(Error('Failed to retrive k8s services inside getHosts call'));
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
                return cb(err);
            }

            const containers = responses.containers;
            const k8s_nodes = responses.nodes.items;

            const containers_by_node = _.flow(
                _.partial(_.map, _, (container) => {
                    const cs_node_name =
                        _.flow(
                            _.partial(_.filter, _, (n) => {
                                return _.get(n, 'metadata.name') === container.host;
                            }),
                            _.first,
                            (node) => {
                                return _.get(node, 'metadata.labels.cs-node-id', 'NOT FOUND!');
                            })(k8s_nodes);

                    return [cs_node_name, container];
                }),
                _.partial(_.reduce, _, (result, node_container) => {
                    const [nodeName, container] = node_container;
                    const containers_for_node = _.get(result, nodeName, []);
                    return _.set(result, nodeName, _.concat(containers_for_node, container));
                }, {}))(containers);

            const cs_nodes = _.map(k8s_nodes, _.flow(
                Translator.csHostFromK8SNode,
                (cs_node) => {
                    return _.merge(cs_node, {
                        containers: containers_by_node[cs_node.id] || []
                    });
                }));

            const keyed = _.keyBy(cs_nodes, 'id');

            return cb(null, keyed);
        });
    }

    getServiceEndpoints(application, cb) {
        if(typeof application === 'function') {
            cb = application;
            application = null;
        }

        const req = {
            uri: `${this.k8s_api_url}/api/v1/namespaces/default/endpoints`,
            method: 'GET',
            json: true
        };

        return request(req, (err, resp, body) => {
            if(err) {
                return cb(err);
            }

            const endpoints = {};

            _.forEach(body.items, (endpoint) => {
                const endpoint_app = _.get(endpoint, 'metadata.name');

                // if specific application endpoint was requested, skip all other endpoints
                if(application !== null && endpoint_app !== application) {
                    return;
                }

                // todo - support multiple subsets and ports
                const subset = _.get(endpoint, 'subsets[0]');
                const port = _.get(subset, 'ports[0].port');

                endpoints[endpoint_app] = _.map(_.get(subset, 'addresses', []), (address) => {
                    return {
                        ip: address.ip,
                        port: port
                    };
                });
            });

            return cb(null, endpoints);
        });
    }

    setDistributedKey(key, value, cb) {
        const req = {
            uri: `${this.etcd_api_url}/v2/keys/${key}`,
            method: 'PUT',
            form: {
                value: JSON.stringify(value)
            }
        };

        return request(req, cb);
    }

    getDistributedKey(key, cb, isWaiting) {
        isWaiting = isWaiting ? true: false;

        const req = {
            uri: `${this.etcd_api_url}/v2/keys/${key}?wait=${isWaiting}`,
            method: 'GET',
            json: true
        };

        request(req, (err, resp, body) => {
            if(err) {
                return cb(err);
            }

            return cb(null, util.safeParse(_.get(body, 'node.value')));
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
