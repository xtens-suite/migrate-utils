#! /usr/bin/env node

var logger = require('./logger.js');
var Migrator = require("./lib/Migrator.js");
logger.log('info', "Creating new migrator");
var migrator = new Migrator();
var DEFAULT_LOCATION = '../xtens-app/assets/dataFiles/tmp';
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
