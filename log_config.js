/**
 * Created by heyuchen on 18-4-17
 */

"use strict";

let logConfig = {
    appenders: { pegasus: {type: "file", filename: "pegasus-nodejs-client.log", maxLogSize: 104857600, backups: 5} },
    categories: { default: { appenders: ["pegasus"], level: "INFO" } }
};
module.exports = logConfig;