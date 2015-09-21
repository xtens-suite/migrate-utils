var utils = require('../lib/utils.js');
var expect = require('chai').expect;
var sinon = require('sinon');
var fs = require('fs');
var path = require('path');
var fileList = ['fileA.txt', 'fileB.xlsx', 'fileC.xlsx', 'fileD.xls', 'fileE.csv', 'fileF.xlsx'];

describe("utils", function() {
    
    describe("#getFilesInFolder", function() {
        
        var fsReadStub, fsStatStub, extnameStub;
        
        before(function() {
            fsReadStub = sinon.stub(fs, 'readdirSync', function() {
                return fileList;
            });
            fsStatStub = sinon.stub(fs, 'statSync', function() {
                return null;
            });
            extnameStub = sinon.stub(path, 'extname', function(name) {
                return "." + name.split(".")[name.split(".").length-1];
            });
        });

        after(function() {
            fs.readdirSync.restore();
            fs.statSync.restore();
            path.extname.restore();
        });      

       it("should return a list of all files", function() {
            var dirPath = "/path/to/folder";
            var res = utils.getFilesInFolder(dirPath);
            expect(res.length).to.eql(fileList.length);
            res.forEach(function(file, i) {
                expect(file).to.equal(dirPath + '/' + fileList[i]);
            });
       });

       it("should return a list of the files matching the given extension", function() {
            var dirPath = "/path/to/folder";
            var res = utils.getFilesInFolder(dirPath, '.xlsx');
            expect(res.length).to.equal(3);
            expect(res[0]).to.equal("/path/to/folder/fileB.xlsx");
            expect(res[1]).to.equal("/path/to/folder/fileC.xlsx");
            expect(res[2]).to.equal("/path/to/folder/fileF.xlsx");
       });

    });

});
