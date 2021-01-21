#! /usr/bin/env node

var loggerGen = require('./../logger.js');
const logger = loggerGen();
var Migrator = require("./lib/Migrator.js");
logger.log('info', "Creating new migrator");
var migrator = new Migrator();
//migrator.migrateCGH("/home/massi/Projects/aCGH/FileBIT",".xlsx")
migrator.migrateAllSubjects()
//migrator.migrateCompleteSubject(5)
.then(function() {
    logger.log('info', 'migrate: done!');
    process.exit(0);
})
.catch(function(err) {
    logger.log('error', err);
    process.exit(1);
});
