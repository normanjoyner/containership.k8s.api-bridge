const _ = require('lodash');
const handlers = require('./handlers');
const KubernetesApi = require('./kubernetes-api');

function register(server, api) {
    server.get('/:apiVersion/cluster', (req, res) => {
        res.send(handlers.cluster.get());
    });

    server.get('/:apiVersion/hosts', (req, res) => {
        api.getHosts((data) => {
            res.json(data);
        });
    });

    server.get('/:apiVersion/applications', (req, res) => {
        api.getApplications((data) => {
            res.json(data);
        });
    });

    server.get('/:apiVersion/applications/:application', (req, res) => {
        res.sendStatus(501);
    });

    server.post('/:apiVersion/applications/:application', (req, res) => {
        api.createApplication(req.body, (code, body) => {
            res.sendStatus(code);
        });
    });

    //Create application containers.
    server.post('/:apiVersion/applications/:application/containers', (req, res) => {
        api.createContainers(
            req.params.application,
            _.merge(
                req.body, 
                req.query.count ? {count: req.query.count} : {}
            ), 
            (code, data) => {
                res.sendStatus(code);
            });
    });
}

module.exports = {register};
