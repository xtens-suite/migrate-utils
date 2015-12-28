#! /usr/bin/env node

var logger = require('./logger.js');
var Migrator = require("./lib/Migrator.js");
logger.log('info', "Creating new migrator");
var migrator = new Migrator();
//migrator.migrateCGH("/home/massi/Projects/aCGH/FileBIT",".xlsx")
return migrator.migrateCGH('/var/xtens/dataFiles/tmp','.xlsx')
//migrator.migrateCompleteSubject(5)
.then(function() {
    logger.log('info', 'migrate: done!');
    process.exit(0);
})
.catch(function(err) {
    logger.log('error', err);
    process.exit(1);
}); 
