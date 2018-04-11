/**
 * Created by heyuchen on 18-2-8
 */

const bunyan = require('bunyan');

// const log = bunyan.createLogger({
//     name: 'pegasus-node-client',
//     streams: [
//         {
//             level: 'info',
//             stream: process.stdout,
//         },
//         {
//             // type: 'rotating-file',
//             // level: 'error',
//             // period : '1d',
//             // count : 3,
//             path: 'error.log',
//             level : 'error',
//         }
//     ],
//     src : true,
//     serializers: {err: bunyan.stdSerializers.err},
// });

const log = bunyan.createLogger({
    "name": "pegasus-node-client",
    "src": "true",
    "streams": [{
        "level": "info",
        // "type": "rotating-file",
        "path": "error.log",
        // "period": 'daily',
        // "count" : 5,
        // "size": 10,
    }]
});

module.exports = log;