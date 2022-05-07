/**
 * Created by heyuchen on 18-4-17
 */

"use strict";

let filename = "./logs/"+process.pid+"/pegasus-nodejs-client.log";
let logConfig = {
    appenders: { pegasus: {type: "file", filename: filename, maxLogSize: 104857600, backups: 10} },
    categories: { default: { appenders: ["pegasus"], level: "INFO" } }
};
module.exports = logConfig;