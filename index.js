const _ = require('lodash');
const express = require('express');
const request = require('request');
const bodyParser = require('body-parser');
const prettyjson = require('prettyjson');

const PluginLoader = require('containership.plugin-loader');
const KubernetesApi = require('./lib/kubernetes-api');
const routes = require('./lib/routes');

const server = express();
server.use(bodyParser.json());

const mode = process.argv[3] ? process.argv[3] : "leader";
const ip = process.argv[4] ? process.argv[4] : "0.0.0.0";

const api = new KubernetesApi(ip, 8080); 
routes.register(server, api);

const testApplication = {
    id: "test",
    image: 'jeremykross/containership.cloud.loadbalancer:latest',
    cpus: 0.1,
    memory: 128,
    network_mode: 'host',
    container_port: 3000,
    host_port: 3001,
    tags: {
        constraints: {
            per_host: 1
        },
        foo: "bar"
    },
    env_vars: {
        CS_LEADER_IP: "127.0.0.1",
        CS_ORCHESTRATOR: "kubernetes"
    },
   volumes: [{
       host: '/var/log/containership',
       container: '/var/log/containership'
   }]
};

const k8sApplication = _.omit(KubernetesApi.csToK8SApp(testApplication), [['spec', 'template']]);

console.log("Conversin: " + JSON.stringify(k8sApplication));
console.log("And back: " + JSON.stringify(KubernetesApi.k8sToCSApp(k8sApplication.spec.template)));

function doRequest(form) {
    console.log("\n");
    request(form, (err, resp, body) => {

        if(err) {
            console.log(`Error ${err}`);
        } else {
            console.log(prettyjson.render(JSON.parse(body)));
        }

        console.log("\n----\n");
    });
}

function installRoutes(server, namespace, routes) {
    _.each(routes, (route) => {
        console.log("Checking route: " + JSON.stringify(route));
        if(route.method === "GET") {
            console.log(`Installing route ${namespace}${route.path} with handler ${route.handler}`);
            server.get(`/:apiVersion/${namespace}${route.path}`, route.handler);
        } else if (route.method === "POST") {
            server.post(`/:apiVersion/${namespace}${route.path}`, route.handler);
        }
    });
}

function startServer() {

    const host = new PluginLoader.K8SHost(mode, "localhost", "9443");;

    PluginLoader.load((plugins) => {

        _.each(plugins, (Plugin) => {
            try {
                const p = new Plugin();
                const routes = p.getApiRoutes(host);

                console.log("Installing routes for: " + p.name + " " + JSON.stringify(routes));

                installRoutes(server, p.name, routes);
            } catch(err) {
                console.log("Error installing routes for plugin: " + err);
            }
        });

        const port = 9443;
        server.listen(port);
        console.log(`Listening on: ${port}`);
    });
}

if(require.main === module && process.argv[2] === 'start') {
    startServer();
}

module.exports = { startServer, KubernetesApi };


