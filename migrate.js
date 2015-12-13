#! /usr/bin/env node

var logger = require('./logger.js');
var Migrator = require("./lib/Migrator.js");
console.log("Creating new migrator");
var migrator = new Migrator();
//migrator.migrateCGH("/home/massi/Projects/aCGH/FileBIT",".xlsx")
migrator.migrateAllSubjects()
// migrator.migrateCompleteSubject(156)
.then(function() {
    logger.log('info', 'migrate: done!');
})
.catch(function(err) {
    logger.log('error', err);
});
