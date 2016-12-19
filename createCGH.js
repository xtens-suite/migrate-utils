#! /usr/bin/env node

var logger = require('./logger.js');
var Migrator = require("./lib/Migrator.js");
logger.log('info', "Creating new migrator");
var migrator = new Migrator();
var DEFAULT_LOCATION = '../xtens-app/assets/dataFiles/tmp';

return migrator.migrateCGH(DEFAULT_LOCATION,'.xlsx')
//migrator.migrateCompleteSubject(5)
.then(function() {
    logger.log('info', 'migrate: done!');
    process.exit(0);
})
.catch(function(err) {
    logger.log('error', err);
    process.exit(1);
});
