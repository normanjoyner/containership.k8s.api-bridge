const _ = require('lodash');
const express = require('express');
const request = require('request');
const bodyParser = require('body-parser');
const prettyjson = require('prettyjson');

const KubernetesApi = require('./lib/kubernetes-api');
const routes = require('./lib/routes');

const server = express();
server.use(bodyParser.json());

const api = new KubernetesApi();
routes.register(server, api);

const testApplication = {
    id: 'swagger-ui',
    image: 'schickling/swagger-ui',
    cpus: 0.25,
    memory: 128,
    container_port: 80,
    engine: 'docker',
    command: '',
    network_mode: 'bridge',
    privileged: 'false',
    respawn: 'true'
};

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

function startServer() {
    const port = 9443;
    server.listen(port);
    console.log(`Listening on: ${port}`);
}

if(process.argv[2] === 'start') {
    startServer();
}

module.exports = { startServer, KubernetesApi };


