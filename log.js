/**
 * Created by hyc on 18-4-23
 */
const log4js = require('log4js');
log4js.configure({
    appenders: { err: { type: 'stderr' } },
    categories: { default: { appenders: ['err'], level: 'WARN' } }
});
let log = log4js.getLogger();

module.exports = log;
