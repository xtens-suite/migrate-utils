#! /usr/bin/env node

var Migrator = require("./lib/Migrator.js");
console.log("Creating new migrator");
var migrator = new Migrator();
migrator.migrateAllSubjects()
.then(function() {
    console.log("Done!");
})
.catch(function(err) {
    console.log(err && err.message);
});
