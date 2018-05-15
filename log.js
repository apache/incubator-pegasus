/**
 * Created by hyc on 18-4-23
 */
"use strict";

const log4js = require('log4js');
log4js.configure({
    appenders: { pegasus: { type: 'file', filename: 'pegasus-nodejs-client.log', maxLogSize: 104857600, backups: 5} },
    categories: { default: { appenders: ['pegasus'], level: 'INFO' } }
});
let log = log4js.getLogger('pegasus');

module.exports = log;
