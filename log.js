/**
 * Created by heyuchen on 18-2-8
 */

const bunyan = require('bunyan');
//const PATH = '/home/heyuchen/work/pegasus-nodejs-client/logs/errors.log';

const log = bunyan.createLogger({
    name: 'pegasus-node-client',
    streams: [
        {
            level: 'info',
            stream: process.stdout,
        }
    ],
    src : true,
});

module.exports = log;