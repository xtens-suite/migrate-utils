/**
 * @author Massimiliano Izzo
 * @description main tool for migration
 */
var _ = require("lodash");
var http = require("http");
var connections = require('../config/connections.js');
var BluebirdPromise = require('bluebird');
// var xlsx = require("xlsx");
var CNV_HEADER_FIRST_CELL_CONTENT = 'AberrationNo';
var utils = BluebirdPromise.promisifyAll(require("./utils.js"));

/**
 * @class
 * @name Migrator
 * @description a set of utility methods to migrate data from the legacy MySQL to the latest PostgreSQL 9.4 database
 */
function Migrator(mysqlConn, pgConn) {
    
    if (!mysqlConn) mysqlConn = 'mysqlLocal';
    if (!pgConn) pgConn = 'postgresqlLocal';

    console.log(connections[mysqlConn]);
    console.log(connections[pgConn]);

    this.knexMysql = require('knex')(connections[mysqlConn]);
    // this.knexMysql.select('ID_PRJ').from('PROJECT').then(console.log).catch(console.log);
    this.knexPg = require('knex')(connections[pgConn]);

    this.subjectMap = {};
    this.sampleMap = {};
    // this.knexPg.select('name').from('data_type').then(console.log).catch(console.log);
    //
    this.dataTypeMap = {
        'Patient':1,
        'Tissue': 2,
        'Fluid':3,
        'DNA':4,
        'RNA':5
    };
}

Migrator.prototype = {

    /**
     * @name migrateProjects
     * @description tool to migrate all the projects
     */
    migrateProjects: function() {

        var knexPg = this.knexPg;

        return this.knexMysql.select('ID_PRJ', 'NAME_PROJECT', 'DESCR_PROJECT').from('PROJECT')
        .orderBy('ID_PRJ')

        .then(function(rows) {
            console.log(rows);
            return BluebirdPromise.each(rows, function(record) {
                console.log(record);
                return knexPg.returning('id').insert({
                    'name': record.NAME_PROJECT,
                    'description': record.DESCR_PROJECT,
                    'created_at': new Date(),
                    'updated_at': new Date() 
                }).into('project');
            }, {concurrency: 1});
        });

    },

    /**
     * @method
     * @name migrateAllSubjects
     */
    migrateAllSubjects: function() {
        var that = this;
        return this.knexMysql.select('ID_PRS_DATA').from('PERSONAL_DATA')

        // once you get all the subjects' IDS
        .then(function(rows) {
            console.log(_.pluck(rows, 'ID_PRS_DATA'));

            // insert each new Subject
            return BluebirdPromise.each(rows, function(subj) {
                var mysqlSubjId = subj && subj.ID_PRS_DATA;
                console.log("Migrator.migrateAllSubject - migrating subject " + mysqlSubjId); 
                return that.migrateCompleteSubject(mysqlSubjId);
            }); 

        }); 
    },

    /**
     * @method
     * @name migrateCompleteSubject
     * @param {Integer} mysqlSubjId - the ID of the subject in MySQL
     */
    migrateCompleteSubject: function(mysqlSubjId) {

        var that = this;
        return this.migrateSubject(mysqlSubjId)

        .then(function(idSubject) {
            console.log("Migrator.migrateCompleteSubject - created new Subject: " + idSubject);
            that.subjectMap[mysqlSubjId] = idSubject;
            // create all the TISSUE (and Fluid?) samples
            return that.migrateSamples(mysqlSubjId);
        });
    },

    /**
     * @name migrateSubject
     * @description tool to migrate a signle subject together with all its data(?) in the database
     * @param{Integer} mysqlId - the ID of the subject in MySQL
     */
    migrateSubject: function(mysqlId) {

        var knexPg = this.knexPg;
        var idSubject;

        return this.knexMysql.select('NAME', 'SURNAME', 'BIRTH_DATE', 'ID_SEX', 'CODE', 'ID_PRJ', 'INSERT_DATE', 'DATE_LAST_UPDATE').from('PERSONAL_DATA')
        .leftJoin('PATIENT', 'PERSONAL_DATA.ID_PRS_DATA', 'PATIENT.ID_PRS_DATA')
        .where('PERSONAL_DATA.ID_PRS_DATA','=',mysqlId)

        .then(function(rows) {
            var record = rows[0];
            return knexPg.transaction(function(trx) {

                // insert Personal Details 
                return knexPg.returning('id').insert({
                    'given_name': record.NAME || " ",
                    'surname': record.SURNAME || " ",
                    'birth_date': record.BIRTH_DATE || '1970-01-01',
                    'created_at': record.INSERT_DATE,
                    'updated_at': record.DATE_LAST_UPDATE
                }).into('personal_details').transacting(trx)

                // insert Subject
                .then(function(ids) {
                    var idPersonalData = ids[0];
                    return knexPg.returning('id').insert({
                        'code': record.CODE,
                        'type': 1, // ID PATIENT TYPE
                        'personal_info': idPersonalData,
                        'sex': record.ID_SEX,
                        'metadata': {},
                        'created_at': record.INSERT_DATE,
                        'updated_at': record.DATE_LAST_UPDATE
                    }).into('subject').transacting(trx);
                })

                // insert project-subject association 
                .then(function(ids) {
                    idSubject = ids[0];
                    return knexPg.insert({'project_subjects': record.ID_PRJ, 'subject_projects': idSubject})
                    .into('project_subjects__subject_projects').transacting(trx);
                });

            })
            .then(function() {
                return idSubject;
            });
        });
    },

    /**
     * @method
     * @name migrateSamples
     * @description migrate primary samples
     */
    migrateSamples: function(mysqlSubjId) {

        var pgSubjId = this.subjectMap[mysqlSubjId];

        console.log("Migrator.migrateSamples - idSubject: " + pgSubjId);

        var knexPg = this.knexPg;
        var that = this;
        var samples;

        return this.knexMysql.select('ID_SAMPLE', 'SAMPLE.ID_SAMPLE_TYPE', 'BIT_CODE', 'ARRIVAL_CODE', 'AP_ARRIVAL_DATE', 'BM_ARRIVAL_DATE',
            'SAMPLE_NAME', 'DESCR_BIT_TISSUE_NAME', 'DESCR_BIT_HISTOPATHOLOGY', 'CELLULARITY', 'QUANTITY',
            'SIZE_FIRST', 'SIZE_SECOND', 'SIZE_THIRD', "CITY", "DESCR_HOSP", "OP_UNIT.DESCRIPTION AS UNIT", "NOTES")
            .from('SAMPLE')
            .leftJoin('HOSPITAL', 'HOSPITAL.ID_HOSP', 'SAMPLE.ID_HOSP')
            .leftJoin('OP_UNIT', 'OP_UNIT.ID_OP_UNIT', 'SAMPLE.ID_OP_UNIT')
            .leftJoin('BIT_HISTOPATHOLOGY', 'BIT_HISTOPATHOLOGY.ID_BIT_HISTOPATHOLOGY', 'SAMPLE.ID_BIT_HISTOPATHOLOGY')
            .leftJoin('BIT_TISSUE_NAME', 'BIT_TISSUE_NAME.ID_BIT_TISSUE_NAME', 'SAMPLE.ID_BIT_TISSUE_NAME')
            .whereNull('ID_PARENT_SAMPLE').whereIn('SAMPLE.ID_SAMPLE_TYPE',['TIS','FLD']).andWhere('ID_PATIENT', mysqlSubjId)

            .then(function(rows) {
                samples = rows;
                // console.log("Migrator.migrateSamples: got these samples: " + samples);
                var primaryIds = _.pluck(samples, 'ID_SAMPLE');        
                // console.log("Migrator.migrateSamples: got these primary IDS:");
                // console.log(primaryIds);
                // retrieve tumour status info if available
                // TODO when I am back check  why this thing DOES NOT WORK!! 
                return that.knexMysql.select('JSON_SCHEMA', 'ID_SAMPLE').from('DATA')
                .leftJoin('SAMPLE_COLLECTION', 'SAMPLE_COLLECTION.ID_SCOLL', 'DATA.ID_SCOLL')
                .leftJoin('SAMPLE_SCOLL', 'SAMPLE_SCOLL.ID_SCOLL', 'SAMPLE_COLLECTION.ID_SCOLL')
                .whereIn('ID_SAMPLE', primaryIds).where('DESCR_DATA', 'TUMOUR STATUS');
            })

            .then(function(tsRows) {
                // console.log("Migrator.migrateSamples - got these tumour statuses: ");
                // console.log(tsRows);
                // console.log("Migrator.migrateSamples - samples: ");
                // console.log(samples);

                // save each (primary) sample
                return BluebirdPromise.each(samples, function(sample, index) {
                    var tumourStatus = _.findWhere(tsRows, {ID_SAMPLE: sample.ID_SAMPLE});
                    // console.log("Migrator.migrateSamples - tumour status found: " + tumourStatus);
                    return that.createPrimary(sample, pgSubjId, tumourStatus);    
                });

            });

    },

    /**
     * @method
     * @name createPrimary
     * @description create a primary sample with all its subsamples
     */
    createPrimary: function(sample, idSubj, tumourStatus) {

        // console.log("Migrator.createPrimary - idSubject: " + idSubj);
        var that = this;
        var knexPg = this.knexPg;
        var sampleType, sampleTypeName, idPrimarySample, ts;

        // change FAT to ADIPOUS TISSUE
        if (sample.DESCR_BIT_TISSUE_NAME === 'FAT') sample.DESCR_BIT_TISSUE_NAME = 'ADIPOUS TISSUE';

        var metadata = {
            arrival_date_mb: {value: sample.BM_ARRIVAL_DATE},
            sample_name: {value: sample.SAMPLE_NAME},
            sample_codification: {value: sample.DESCR_BIT_TISSUE_NAME},
            pathology: {value: sample.DESCR_BIT_HISTOPATHOLOGY},
            hospital: {value: [sample.CITY, sample.DESCR_HOSP].join(" - ")},
            unit: {value: sample.UNIT}
        };

        switch (sample.ID_SAMPLE_TYPE) {
            case 'TIS':
                metadata.arrival_code = {value: sample.ARRIVAL_CODE};
                metadata.arrival_date_pa = {value: sample.AP_ARRIVAL_DATE};
                metadata.size_x = {value: sample.SIZE_FIRST, unit: 'mm'};
                metadata.size_y = {value: sample.SIZE_SECOND, unit: 'mm'};
                metadata.size_z = {value: sample.SIZE_THIRD, unit: 'mm'};
                metadata.tumour_cellularity = {value: sample.CELLULARITY, unit: '%'};
                sampleTypeName = 'Tissue';
                break;
            case 'FLD':
                metadata.sampling_date = {value: sample.AP_ARRIVAL_DATE};
                metadata.quantity = {value: sample.QUANTITY, unit: 'ml'};
                sampleTypeName = 'Fluid';
        }

        // if the tumour status is not empty
        if (!_.isEmpty(tumourStatus)) {
            var jsonSchema = JSON.parse(tumourStatus.JSON_SCHEMA);
            metadata.tumour_status = {value: jsonSchema.body[0].content[0].instances[0].value};
        }
        else {
            try {
                ts = sample.NOTES.split("\n")[0].split(':')[1].trim();
            }
            catch(err) {
                console.log(err);
                ts = null;
            }
            console.log(ts);
            metadata.tumour_status = {value: ts};
        }

        console.log("Migrator.createPrimary - metadata: " + metadata);
        return knexPg.select('id').from('data_type').where('name', sampleTypeName)

        .then(function(rows) {
            if (rows && rows.length) {
                sampleType = rows[0].id;
                return knexPg.returning('id').insert({
                    'type': sampleType,
                    'biobank': 1,       // NOTA BENE: there is only one biobank!!
                    'metadata': metadata,
                    'biobank_code': sample.BIT_CODE,
                    'parent_subject': idSubj,
                    'created_at': new Date(),
                    'updated_at': new Date()
                }).into('sample'); 
            }
        })

        .then(function(ids) { 
            idPrimarySample = ids[0];
            that.sampleMap[sample.ID_SAMPLE] = idPrimarySample;
            // console.log("Migrator.createPrimary: id = " + idPrimarySample);
            if (sampleTypeName === 'Fluid') {
                // console.log("Sample Type is: " + sampleType);
                return that.migratePlasmaSamples(sample.ID_SAMPLE, idSubj, sampleType);
            }
            else
                return null;
        })

        .then(function() {
            return that.migrateNucleicDerivatives(sample.ID_SAMPLE, idSubj);
        });

    },

    /**
     * @method
     * @name migratePlasmaSamples
     */
    migratePlasmaSamples: function(mysqlSampleId, idSubj, sampleType) {

        var that = this;

        return this.knexMysql.select(
            'ID_SAMPLE', 'SAMPLE.ID_SAMPLE_TYPE', 'BIT_CODE', 'ARRIVAL_CODE', 'AP_ARRIVAL_DATE', 'BM_ARRIVAL_DATE',
            'SAMPLE_NAME', 'DESCR_BIT_TISSUE_NAME', 'DESCR_BIT_HISTOPATHOLOGY', 'CELLULARITY', 'QUANTITY',
            'SIZE_FIRST', 'SIZE_SECOND', 'SIZE_THIRD', "CITY", "DESCR_HOSP", "OP_UNIT.DESCRIPTION AS UNIT", "NOTES")
            .from('SAMPLE')

            .leftJoin('HOSPITAL', 'HOSPITAL.ID_HOSP', 'SAMPLE.ID_HOSP')
            .leftJoin('OP_UNIT', 'OP_UNIT.ID_OP_UNIT', 'SAMPLE.ID_OP_UNIT')
            .leftJoin('BIT_HISTOPATHOLOGY', 'BIT_HISTOPATHOLOGY.ID_BIT_HISTOPATHOLOGY', 'SAMPLE.ID_BIT_HISTOPATHOLOGY')
            .leftJoin('BIT_TISSUE_NAME', 'BIT_TISSUE_NAME.ID_BIT_TISSUE_NAME', 'SAMPLE.ID_BIT_TISSUE_NAME')
            .from("SAMPLE")
            .where('ID_PARENT_SAMPLE', mysqlSampleId).andWhere('DESCR_BIT_TISSUE_NAME','PLASMA')

            .then(function(rows) {
                // console.log("Migrator.migratePlasmaSamples: got these samples: " + rows);

                return BluebirdPromise.each(rows, function(sample) {
                    var metadata = {
                        arrival_date_mb: {value: sample.BM_ARRIVAL_DATE},
                        sample_name: {value: sample.SAMPLE_NAME},
                        sample_codification: {value: sample.DESCR_BIT_TISSUE_NAME},
                        pathology: {value: sample.DESCR_BIT_HISTOPATHOLOGY},
                        hospital: {value: [sample.CITY, sample.DESCR_HOSP].join(" - ")},
                        unit: {value: sample.UNIT},
                        sampling_date: {value: sample.AP_ARRIVAL_DATE},
                        quantity: {value: sample.QUANTITY, unit: 'ml'}
                    };

                    return that.knexPg.returning('id').insert({
                        'type': sampleType,
                        'biobank': 1,
                        'metadata': metadata,
                        'biobank_code': sample.BIT_CODE,
                        'parent_subject': idSubj,
                        'parent_sample': that.sampleMap[mysqlSampleId],
                        'created_at': new Date(),
                        'updated_at': new Date()
                    }).into('sample');

                });

            });

    },

    /**
     * @method
     * @name migrateNucleicDerivatives
     * @description migrate DNA and RNA samples from MySQL to PostgreSQL (>= 9.4) database
     */
    migrateNucleicDerivatives: function(mysqlSampleId, idSubj) {
        var that = this;
        var metadata, derivatives;

        return this.knexMysql.select('SAMPLE.ID_SAMPLE','BIT_CODE', 'ARRIVAL_CODE', 'ID_SAMPLE_TYPE', 'QUANTITY', 'EXTRACTION_DATE', 'CONCENTRATION')
        .from('SAMPLE')
        .whereIn('ID_SAMPLE_TYPE', ['DNA','RNA']).andWhere('ID_PARENT_SAMPLE', mysqlSampleId)

        .then(function(rows) {
            derivatives = rows;
            // console.log("Migrator.migrateNucleicDerivatives - got these samples: "); 
            // console.log(derivatives);
            var derivativesId = _.pluck(rows, 'ID_SAMPLE');
            return that.knexMysql.select('JSON_SCHEMA', 'ID_SAMPLE').from('DATA')
            .leftJoin('SAMPLE_COLLECTION', 'SAMPLE_COLLECTION.ID_SCOLL', 'DATA.ID_SCOLL')
            .leftJoin('SAMPLE_SCOLL', 'SAMPLE_SCOLL.ID_SCOLL', 'SAMPLE_COLLECTION.ID_SCOLL')
            .whereIn('DESCR_DATA', ['QUALITY CONTROL - DNA', 'QUALITY CONTROL - RNA'])
            .whereIn('ID_SAMPLE', derivativesId);
            /*
               return BluebirdPromise.each(derivatives, function(sample) {
               return that.knexMysql.select('JSON_SCHEMA').from('DATA')
               .leftJoin('SAMPLE_COLLECTION', 'SAMPLE_COLLECTION.ID_SCOLL', 'DATA.ID_SCOLL')
               .leftJoin('SAMPLE_SCOLL', 'SAMPLE_SCOLL.ID_SCOLL', 'SAMPLE_COLLECTION.ID_SCOLL')
               .whereIn('DESCR_DATA', ['QUALITY CONTROL - DNA', 'QUALITY CONTROL - RNA']).andWhere('ID_SAMPLE',sample.ID_SAMPLE);
               }); */

        })

        .then(function(qcRows) {
            var jsonSchema;
            // console.log("Migrator.migrateNucleicDerivatives - qcRows: ");
            // console.log(qcRows);
            return BluebirdPromise.each(derivatives, function(derivative) {
                var qc = _.findWhere(qcRows, {'ID_SAMPLE': derivative.ID_SAMPLE});
                // console.log("Migrator.migrateNucleicDerivatives - qc: ");
                // console.log(qc);

                metadata = {
                    arrival_code: {value: derivative.ARRIVAL_CODE},
                    sampling_date: {value: derivative.EXTRACTION_DATE},
                    quantity: {value: derivative.QUANTITY, unit: 'µg'},
                    concentration: {value: derivative.CONCENTRATION, unit: 'ng/µl'}
                };

                if (!_.isEmpty(qc)) {
                    jsonSchema = JSON.parse(qc.JSON_SCHEMA);
                    metadata.quality = {value: jsonSchema.body[0].content[0].instances[0].value};
                    metadata.kit_type = {value: jsonSchema.body[0].content[1].instances[0].value};
                    metadata.$260_280 = {value: jsonSchema.body[0].content[2].instances[0].value};
                    metadata.$260_230 = {value: jsonSchema.body[0].content[3].instances[0].value};
                }

                // console.log("Migrator.migrateNucleicDerivatives - inserting derivative sample: ");
                // console.log(metadata);

                return that.knexPg.returning('id').insert({
                    'type': that.dataTypeMap[derivative.ID_SAMPLE_TYPE],
                    'biobank': 1,
                    'biobank_code': derivative.BIT_CODE,
                    'parent_subject': idSubj,
                    'parent_sample': that.sampleMap[mysqlSampleId],
                    'metadata': metadata,
                    'created_at': new Date(),
                    'updated_at': new Date()
                }).into('sample');
            });

        })

        .then(function(ids) {
            // console.log("Migrator.migrateNucleicDerivatives - migrated:" + ids);
            return true;
        });

    },

    /**
     * @method
     * @name migrateCGH
     * @description migrates all the CGH files in a given folder and saves them on XTENS database
     * @param{string} folder - the absolute path of the folder containing all the CGH files
     * @param{string} ext - the extension of the files (XLSX, XLS)
     */
    migrateCGH: function(folder, ext) {
        var that = this;
        var files = utils.getFilesInFolder(folder, ext);
        console.log(files);
        return BluebirdPromise.map(files, function(file, index) {
            console.log(file);
            utils.composeCGHMetadataAsync(file)
            .spread(function(sampleCode, acghProcessed, cnvArray) {
                var param = '{"arrival_code": {"value": "' + sampleCode + '"}}';
                console.log(param);
                return that.knexPg.raw("SELECT metadata->'arrival_code'->>'value' AS arrival_code FROM sample WHERE metadata @> ?", 
                                      [param])
                                      .then(function(resp) {
                                        console.log("Migrator.migrateCGH - sample query results: ");
                                        console.log(resp);
                                      });
            })
            .catch(function(error) {
                console.log("Error caught: " + error.message);
            });
        });
    },

    /**
     * @method
     * @name composeCGHMetadata
     * @description migrates a CGH data from an Excel file to the PostgreSQL database
     * @param{string} - filePath: the file path
     *
    composeCGHMetadata: function(filePath, callback) {
        var z;
        var fileName = filePath.split("/")[filePath.split("/").length-1];
        var sampleCode = fileName.split(".")[0];
        console.log(sampleCode);
        var workbook = xlsx.readFile(filePath);
        var worksheet = workbook.Sheets[workbook.SheetNames[0]];
        var range = xlsx.utils.decode_range(worksheet['!ref']);
        console.log(range);
        var c, r, firstCellInRow, cell, cellElems;
        var acghProcessedFields = [];
        for (r=range.s.r; r<=range.e.r; r++) {
            firstCellInRow = worksheet[xlsx.utils.encode_cell({c:0, r:r})];
            console.log(firstCellInRow);
            if (firstCellInRow && firstCellInRow.v && firstCellInRow.v.split) {
                if (firstCellInRow && firstCellInRow.v === 'AberrationNo') {
                    break;
                }
                cellElems = firstCellInRow.v.split(':').map(Function.prototype.call, String.prototype.trim);
                acghProcessedFields.push(cellElems);
            }
            else continue;
        }
        var metadata = utils.composeCGHProcessedMetadata(acghProcessedFields);
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
            cnvArr.push(utils.composeCNVMetadata(cellElems));
            // console.log(_.omit(cnvMetadata, ['gene_name', 'mirna']));
        }
        return [sampleCode, acghProcessedFields, _.compact(cnvArr)];

    } */

};

module.exports = Migrator;
