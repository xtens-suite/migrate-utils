#! /usr/bin/env node

var loggerGen = require('./logger.js');
const logger = loggerGen();
var Migrator = require("./lib/Migrator.js");
logger.log('info', "Creating new migrator");
var migrator = new Migrator();
// var DEFAULT_LOCATION = '/mnt/www';
var DEFAULT_LOCATION = '/mnt/projects';
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
process.env.NGSPrefixDebugPath = '';
process.env.NGSPrefixDebugSourceFilesPath = '';
// process.env.NGSPrefixDebugSourceFilesPath = '/run/user/1000/gvfs/sftp:host=10.116.13.67,user=xtens';

// logger.log('info', process.argv[2]);
process.argv[2] = JSON.stringify({
    "owner": 45,
    "executor": 7,
    "folder": "wes_master",
    // "folder": "test",
    "bearerToken": ""
});
migrator.migrateMasterNGSVCF(DEFAULT_LOCATION, '.gz', process).then((summary) => {
    logger.log('info', 'migrate: done!');
    // process.send(summary, function () {
    if (summary && summary.error) {
        process.exit(1);
    }
    process.exit(0);
    // });
})
    .catch(function (err) {
        console.log(err);
        logger.log('error', err);
        process.exit(1);
    });
