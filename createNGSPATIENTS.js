#! /usr/bin/env node

var loggerGen = require('./logger.js');
const logger = loggerGen();
var Migrator = require("./lib/Migrator.js");
logger.log('info', "Creating new migrator");
var migrator = new Migrator();
var DEFAULT_LOCATION = '/mnt/xtens-filesystem/landing';
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
process.env.NGSPrefixDebugPath = '';

migrator.migrateNGSPATIENTS(DEFAULT_LOCATION, '.xlsm', process)
    .then(function (summary) {
        logger.log('info', 'migrate: done!');
        process.send(summary, function () {
            if (summary.error) {
                process.exit(1);
            }
            process.exit(0);
        });
    })
    .catch(function (err) {
        logger.log('error', err);
        process.exit(1);
    });
