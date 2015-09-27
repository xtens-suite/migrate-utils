var expect = require('chai').expect;
var sinon = require('sinon');

var Migrator = require('./../lib/Migrator.js');

describe('Migrator', function() {
    
    describe('#constructor', function() {
       
        var migrator = new Migrator();

    });
    
    /*
    describe("#composeCGHMetadata", function() {
        
        var migrator = new Migrator();
        migrator.composeCGHMetadata('/home/massi/Projects/aCGH/FileBIT/15-H-00455.xlsx');

    }); */

    describe("#migrateCGH", function() {
        var migrator = new Migrator();
        migrator.migrateCGH('/home/massi/Projects/aCGH/FileBIT', '.xlsx')
        .then(function() {
            console.log("done");
        })
        .catch(function(err) {
            console.log(err);
        });
    });

    describe("#testMigrate", function() {
        it("should migrate a subject", function() {
            console.log("Migrator-test.testMigrate - here we are");
            var migrator = new Migrator();
            return migrator.migrateCompleteSubject(1).then(function() {
                console.log("Migrator-test.testMigrate - done");
            })
            .catch(console.log);
        });
    });

});
