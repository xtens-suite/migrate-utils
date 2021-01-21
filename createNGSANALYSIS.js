#! /usr/bin/env node

var loggerGen = require('./logger.js');
const logger = loggerGen();
var Migrator = require("./lib/Migrator.js");
logger.log('info', "Creating new migrator");
var migrator = new Migrator();
var DEFAULT_LOCATION = '/mnt/xtens-filesystem/landing';
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
process.env.NGSPrefixDebugPath = '';

return migrator.createNGSANALYSIS(DEFAULT_LOCATION, '.tsv', process)
    .then(function (summary) {
        logger.info('migrate: done!');
        // process.send(summary, function () {
        //     if (summary.error) {
        //     }
        // });
        return;
    })
    .catch(function (err) {
        logger.log('error', err.message);
        // logger.error(err);
        return;
    });
