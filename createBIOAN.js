#! /usr/bin/env node

var loggerGen = require('./logger.js');
const logger = loggerGen();
var Migrator = require("./lib/Migrator.js");
logger.log('info', "Creating new migrator");
var migrator = new Migrator();
var DEFAULT_LOCATION = '../xtens-app/assets/dataFiles/tmp';
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

migrator.migrateBioAn(DEFAULT_LOCATION, '.xlsx', process)
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
