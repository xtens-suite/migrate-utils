/**
 * @author Massimiliano Izzo
 */
var _ = require("lodash");
var fs = require("fs");
var path = require("path");
var xlsx = require("xlsx");
var metadataFieldNameNotAllowedCharset = /[^A-Za-z_$0-9:]/g;
var floatFields = ['Threshold', 'Centralization (legacy) Threshold'];
var integerFields = ['Centralization (legacy) Bin Size'];
var booleanFields = ['Fuzzy Zero', 'GC Correction', 'Centralization (legacy)', 'Diploid Peak Centralization',
    'Manually Reassign Peaks', 'Combine Replicates (Intra Array)', 'Combine Replicates (Inter Array)',
    'Genomic Boundary', 'Show Flat Intervals'];

/**
 * @method
 * @name getFilesInFolder
 * @param{string} folder - the folder where you search for files
 * @param{string} ext - extension to be matched
 * @param{boolean} isDeep - if true search subfolders as well 
 * @return{Array} files - the list of files in the directory
 */
    function getFilesInFolder(folder, ext, isDeep) {
        var res = [];
        fs.readdirSync(folder).forEach(function(file) {

            file = folder + "/" + file;
            var stat = fs.statSync(file);

            if (stat && stat.isDirectory()) {
                if (isDeep) {
                    res = res.concat(getFilesInFolder(file));
                }
            }
            else if (!ext || path.extname(file) === ext) {
                console.log("Pushing file: " + file);
                res.push(file);
            }

        });
        return res;
    }

/**
 * @method
 * @name formatMetadataFieldName
 * @param{string} - name: unformatted metadata field name
 * @return{string} - formatted metadata field name
 */
    function formatMetadataFieldName(name) {
        // if name starts with digit add a dollar char ($) at the beginning
        if (/^\d/.test(name)) {
            name = "$" + name;
        }

        // replace with underscore all the not allowed charsets 
        // (all the chars that  cannot be used in Javascript property names with dot notation)
        return name.toLowerCase().replace(metadataFieldNameNotAllowedCharset, "_");
    }

/**
 * @method
 * @name composeCGHProcessedMetadata
 * @description construct the metadata object for the CGH Processed data type
 * @param{Array} cghMetadataArr: a 2D array. Each nested array contains two elements the first is the metadata field name
 *              the second its value
 * @return{Object} the metadata object ready to be stored in the database
 */
    function composeCGHProcessedMetadata(cghMetadataArr) {
        var res = {};
        _.each(cghMetadataArr, function(metadatumArr) {
            if (metadatumArr[0] === 'Window Size') {
                var value = parseFloat(metadatumArr[1].match(/\d+/)[0]);
                console.log("Window Size value: " + value);
                if (!_.isNaN(value)) {
                    res[formatMetadataFieldName(metadatumArr[0])] = {
                        value: value, 
                        unit: metadatumArr[1].replace(/\d/g, '').toLowerCase()
                    };
                }
                else {
                    throw new Error("utils.composeCGHProcessedMetadata - Could not correctly parse Window Size");
                }
            }
            else if (floatFields.indexOf(metadatumArr[0]) > -1) {
                res[formatMetadataFieldName(metadatumArr[0])] = {value: parseFloat(metadatumArr[1])};
            }
            else if (integerFields.indexOf(metadatumArr[0]) > -1) {
                res[formatMetadataFieldName(metadatumArr[0])] = {value: parseInt(metadatumArr[1])};
            }
            else if (booleanFields.indexOf(metadatumArr[0]) > -1) {
                res[formatMetadataFieldName(metadatumArr[0])] = {
                    value: metadatumArr[1] === 'ON' ? true : false
                };
            }
            else {
                res[formatMetadataFieldName(metadatumArr[0])] = {value: metadatumArr[1]};
            }
        });
        return res;
    }

/**
 * @method
 * @name composeCNVMetadata
 * @param{Array} - the array containing the metadata fields. They are retrieved from the Excel file in this order:
 *                0: aberration number (skipped)
 *                1: chromosome
 *                2: cyto band (start - end): to be split
 *                3: position start
 *                4: position stop
 *                5: # probes
 *                6: amplification
 *                7: deletion
 *                8: pval
 *                9: Gene Names
 *                10: CNV
 *                11: miRNA
 * @return{Object} - the CNV metadata object according to XTENS metadata format
 */
    function composeCNVMetadata(cnvArr) {
        if (cnvArr.length < 12 || !cnvArr[1]) {
            return; 
        }
        var metadata = {};
        metadata.chr = {value: cnvArr[1]};
        var cytobands = cnvArr[2].split('-').map(Function.prototype.call, String.prototype.trim);
        metadata.cytoband_start = {value: cytobands[0]};
        // if there is only a band, stop band is just the same start band
        var cytobandStop = cytobands.length > 1 ? cytobands[1] : cytobands[0];
        metadata.cytoband_stop = {value: cytobandStop};
        metadata.start = {value: cnvArr[3]};
        metadata.stop = {value: cnvArr[4]};
        metadata._probes = {value: cnvArr[5]};
        var ampl = parseFloat(cnvArr[6].toString().replace(',','.'));
        metadata.is_amplification = {value: ampl > 0};
        metadata.amplification = {value: ampl};
        var del = parseFloat(cnvArr[7].toString().replace(',','.'));
        metadata.is_deletion = {value: del < 0};
        metadata.deletion = {value: del};
        metadata.pval = {value: Number(cnvArr[8].toString().replace(",",'.'))};
        metadata.gene_name = {values: cnvArr[9] && cnvArr[9].split(",").map(Function.prototype.call, String.prototype.trim)};
        metadata.mirna = {values: cnvArr[11] && cnvArr[11].split(",").map(Function.prototype.call, String.prototype.trim)};
        return metadata;

    }

    
    /**
     * @method
     * @name composeCGHMetadata
     * @description migrates a CGH data from an Excel file to the PostgreSQL database
     * @param{string} - filePath: the file path
     * @return{Array} - array with three components
     *                  0 - sample_code (ARRIVAL/BIT code)
     *                  1 - aCGH processed JSON metadata
     *                  2 - array with all CNVs metadata objects
     */
    function composeCGHMetadata(filePath) {
        var z;
        var fileName = filePath.split("/")[filePath.split("/").length-1];
        var sampleCode = fileName.split(".")[0];
        console.log(sampleCode);
        var workbook = xlsx.readFile(filePath);
        var worksheet = workbook.Sheets[workbook.SheetNames[0]];
        var range = xlsx.utils.decode_range(worksheet['!ref']);
        //  console.log(range);
        var c, r, firstCellInRow, cell, cellElems;
        var acghProcessedFields = [];
        for (r=range.s.r; r<=range.e.r; r++) {
            firstCellInRow = worksheet[xlsx.utils.encode_cell({c:0, r:r})];
            // console.log(firstCellInRow);
            if (firstCellInRow && firstCellInRow.v && firstCellInRow.v.split) {
                if (firstCellInRow && firstCellInRow.v === 'AberrationNo') {
                    break;
                }
                cellElems = firstCellInRow.v.split(':').map(Function.prototype.call, String.prototype.trim);
                acghProcessedFields.push(cellElems);
            }
            else continue;
        }
        var metadata = composeCGHProcessedMetadata(acghProcessedFields);
        // console.log(metadata);

        // Parse CNV Field Names - NOT USED now
        var cnvFieldNames = {};
        for (c=range.s.c; c<=range.e.c; c++) {
            cnvFieldNames[ worksheet[xlsx.utils.encode_cell({c:c, r:r})].v ] = c;
        }
        // console.log(cnvFieldNames);

        var cnvArr = [];
        // for each row containing a CNV record
        for (r=r+1; r<=range.e.r; r++) {
            cellElems = [];
            // put all values in an array
            for (c=range.s.c; c<=range.e.c; c++) {
                cell = worksheet[xlsx.utils.encode_cell({c:c, r:r})];
                if (cell) {
                    cellElems.push(cell.v);
                }
                else {
                    cellElems.push(null);
                }
            }
            // console.log(cellElems);
            // and compose CNV metadata for that record (i.e. array);
            cnvArr.push(composeCNVMetadata(cellElems));
            // console.log(_.omit(cnvMetadata, ['gene_name', 'mirna']));
        }
        return {
            sampleCode: sampleCode, 
            acghProcessed: acghProcessedFields, 
            cnvArr: _.compact(cnvArr)
        };

    }
    
    module.exports.composeCGHProcessedMetadata = composeCGHProcessedMetadata;
    module.exports.composeCNVMetadata = composeCNVMetadata;
    module.exports.getFilesInFolder = getFilesInFolder;
    module.exports.composeCGHMetadata = composeCGHMetadata;
