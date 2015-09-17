var expect = require('chai').expect;
var sinon = require('sinon');

var Migrator = require('./../lib/Migrator.js');

describe('Migrator', function() {
    
    describe('#constructor', function() {
       
        var migrator = new Migrator();

    });

    describe("#migrateCGHRecord", function() {
        
        var migrator = new Migrator();
        migrator.migrateCGHRecord('/home/massi/Projects/aCGH/FileBIT/15-H-00455.xlsx');

    });

});
