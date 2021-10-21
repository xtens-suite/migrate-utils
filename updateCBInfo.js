#! /usr/bin/env node

var loggerGen = require('./logger.js');
const logger = loggerGen();
var Migrator = require("./lib/Migrator.js");
logger.log('info', "Creating new migrator");
var migrator = new Migrator();
var DEFAULT_LOCATION = '../xtens-file-system/landing'; //'/var/www/xtens-app/assets/dataFiles/tmp' '../xtens-app/assets/dataFiles/tmp' '../xtens-file-system/landing'
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

return migrator.importCNBInfo(DEFAULT_LOCATION, '.xlsx')
  //migrator.migrateCompleteSubject(5)
  .then(function (summary) {
    logger.log('info', 'migrate: done!');
    process.send(summary, function () {
      process.exit(0);
    });

  })
  .catch(function (err) {
    logger.log('error', err);
    process.exit(1);
  });
