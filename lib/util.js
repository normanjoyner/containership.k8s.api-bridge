'use strict';

function safeParse(jsonString) {
    try {
        return JSON.parse(jsonString);
    } catch (err) {
        return jsonString;
    }
}

function ifSuccessfulResponse(cb) {
    return (err, resp, body) => {
        if(!err) {
            cb(safeParse(body));
        } else {
            // eslint-disable-next-line no-console
            console.log(`Failed RESPONSE in wrapper: ${err}`);
        }
    };
}

module.exports = { ifSuccessfulResponse, safeParse };
