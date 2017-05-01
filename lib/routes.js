const _ = require('lodash');
const request = require('request');
const AsyncLock = require('async-lock');

const KubernetesApi = require('./kubernetes-api');

const lock = new AsyncLock();

let etcdToken;
let freeEtcdMemberLock;

function register(server, api) {
    server.get('/:apiVersion/cluster', (req, res) => {
        res.send({});
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
            res.json({});
        });
    });

    server.delete('/:apiVersion/applications/:application', (req, res) => {
        api.deleteApplication(req.params.application, (code, body) => {
            res.sendStatus(code);
        });
    });

    server.put('/:apiVersion/applications/:application', (req, res) => {
        api.updateApplication(req.params.application, req.body, (code, body) => {
            res.status(code).json({});
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
                res.sendStatus(201);
            });
    });

    //Delete application container
    server.delete('/:apiVersion/applications/:application/containers/:container', (req, res) => {
        api.deleteContainer(req.params.application, req.params.container, (code, data) => {
            res.sendStatus(code);
        });
    });

    server.get('/:apiVersion/variables', (req, res) => {
        res.json({});
    });

    server.get('/extended/v1/etcdToken', (req, res) => {
        lock.acquire('etcdToken', (done) => {
            
            if(etcdToken) return done();

            request({
                url: 'http://localhost:2379/version', 
                json: true
            }, (err, response) => {
                const clusterVersion = _.get(response, ["body", "etcdcluster"], "not_decided");
                etcdToken = clusterVersion  === "not_decided" ? null : clusterVersion;
                done();
            });

        }).then(() => {
            res.status(etcdToken ? 200 : 500).send(etcdToken);
        });
    });

    server.get('/extended/v1/etcdMembers', (req, res) => {
        request({
            url:'http://127.0.0.1:2379/v2/members',
            json: true,
            method: "GET",
        }, (err, localResponse, body) => {
            res.status(localResponse.statusCode).json(body);
        });
    });

    server.post('/extended/v1/etcdMembers', (req, res) => {
        lock.acquire('etcdMembers', (done) => {
            freeEtcdMemberLock = done;
            request({
                url:'http://127.0.0.1:2379/v2/members',
                json: true,
                method: "POST",
                body: req.body,
            }, (err, localResponse, body) => {
                res.status(localResponse.statusCode).json(body);
            });
        });
    });

    server.post('/extended/v1/freeEtcdLock', (req, res) => {
        if(freeEtcdMemberLock) {
            console.log("Freeing etcd lock!")

            const done = freeEtcdMemberLock;
            freeEtcdMemberLock = null;
            done();

            res.status(200).send("FREED");
        }

        res.status(200).send("NOT ACQUIRED");

    });


    server.post('/extended/v1/enforce-constraints', (req, res) => {
        api.enforceAllConstraints();
        res.send("enforced?");
    });
}

module.exports = {register};
