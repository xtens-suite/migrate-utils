/**
 * @author Massimiliano Izzo
 * @description main tool for migration
 */
/* jshint node:true */
/* jshint esnext: true */
"use strict";

let basePath = 'http://localhost:1337';
let _ = require("lodash");
let http = require("http");
let connections = require('../config/connections.js');
let BluebirdPromise = require('bluebird');
let utils = require("./utils.js");
let allowedTumourStatuses = ["ONSET", "POST-CHEMO", "RELAPSE", "POST-CHEMO RELAPSE"];
let allowedQualities = ["GOOD","AVERAGE","POOR","N.D."];
let request = BluebirdPromise.promisify(require("request"));
let logger = require('../logger.js');
let moment = require('moment-timezone');
let xlsx = require("xlsx");


const CNV_HEADER_FIRST_CELL_CONTENT = 'AberrationNo';
const OK = 200;
const CREATED = 201;
const MICROARRAY_RAW = 'MICROARRAY - RAW';
const MICROARRAY_MAS5 = 'MICROARRAY - MAS5';
const MICROARRAY_NB = 'MICROARRAY - NB';
const ALIQUOT_DELIVERY = 'ALIQUOT DELIVERY';
const ALK_MUTATION = 'ALK - MUTATION';

const NB_CLINICAL_SITUATION_POSTGRES_ID = 16;

function mapReportValue(val) {
    let res = val === 'NORMAL' ? 'INTERMEDIATE' : val;
    return res;
}

function formatDate(val) {
    if (!val) return;
    return moment.tz(val, "Europe/Rome").format("YYYY-MM-DD");
}

BluebirdPromise.promisifyAll(request, {
    multiArgs: true
});

/**
 * @class
 * @name Migrator
 * @description a set of utility methods to migrate data from the legacy MySQL to the latest PostgreSQL 9.4 database
 */
function Migrator(mysqlConn, pgConn) {

    if (!mysqlConn) mysqlConn = 'mysqlLocal';
    if (!pgConn) pgConn = 'postgresqlLocal';

    logger.info(connections[mysqlConn]);
    logger.info(connections[pgConn]);

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

        let knexPg = this.knexPg;

        return this.knexMysql.select('ID_PRJ', 'NAME_PROJECT', 'DESCR_PROJECT').from('PROJECT')
        .orderBy('ID_PRJ')

        .then(function(rows) {
            // console.log(rows);
            return BluebirdPromise.each(rows, function(record) {
                // console.log(record);
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
        let that = this;
        return this.knexMysql.select('ID_PRS_DATA').from('PERSONAL_DATA')

        // once you get all the subjects' IDS
        .then(function(rows) {
            // console.log(_.pluck(rows, 'ID_PRS_DATA'));

            // insert each new Subject
            return BluebirdPromise.each(rows, function(subj) {
                let mysqlSubjId = subj && subj.ID_PRS_DATA;
                logger.log('info',"Migrator.migrateAllSubject - migrating subject " + mysqlSubjId);
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

        let that = this, idSubject;
        return this.migrateSubject(mysqlSubjId)

        .then(function(subjId) {
            idSubject = subjId;
            that.subjectMap[mysqlSubjId] = idSubject;
            return that.migrateNBClinicalData(mysqlSubjId, idSubject);
        })

        .then(function() {
            console.log("Migrator.migrateCompleteSubject - created new Subject: " + idSubject);
            // that.subjectMap[mysqlSubjId] = idSubject;
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

        let knexPg = this.knexPg;
        let idSubject;

        let query = this.knexMysql.select('NAME', 'SURNAME', 'BIRTH_DATE', 'ID_SEX', 'CODE', 'ID_PRJ', 'INSERT_DATE', 'DATE_LAST_UPDATE')
        .from('PERSONAL_DATA')
        .leftJoin('PATIENT', 'PERSONAL_DATA.ID_PRS_DATA', 'PATIENT.ID_PRS_DATA')
        .where('PERSONAL_DATA.ID_PRS_DATA','=',mysqlId);

        logger.log('info', query.toString());

        return query.then(function(rows) {
            let record = rows[0];
            return knexPg.transaction(function(trx) {
                // insert Personal Details
                return knexPg.returning('id').insert({
                    'given_name': record.NAME || " ",
                    'surname': record.SURNAME || " ",
                    'birth_date': formatDate(record.BIRTH_DATE) || '1970-01-01',
                    'created_at': formatDate(record.INSERT_DATE),
                    'updated_at': formatDate(record.DATE_LAST_UPDATE)
                }).into('personal_details').transacting(trx)

                // insert Subject
                .then(function(ids) {
                    let idPersonalData = ids[0];
                    return knexPg.returning('id').insert({
                        'code': record.CODE,
                        'type': 1, // ID PATIENT TYPE
                        'personal_info': idPersonalData,
                        'sex': record.ID_SEX,
                        'metadata': {},
                        'created_at': formatDate(record.INSERT_DATE),
                        'updated_at': formatDate(record.DATE_LAST_UPDATE)
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
     * @name migrateNBClinicalData
     * @description migrate Neuroblastoma Clinical Data for the given sample
     * @param{integer} - mysqlSubjId: the subject ID on MySQL database
     */
    migrateNBClinicalData: function(mysqlSubjId, pgSubjId) {
        let knexPg = this.knexPg;

        let query = this.knexMysql.select('NB_CLINICAL_SITUATION.*', 'CLINICAL_PROTOCOL.NAME_CLINICAL_PROTOCOL',
                                          'NB_HISTOLOGY.DESCR_NB_HISTOLOGY', 'NB_PRIMARY_SITE.DESCR_NB_PRIMARY_SITE')
                                          .from('NB_CLINICAL_SITUATION')
                                          .leftJoin('CLINICAL_PROTOCOL', 'NB_CLINICAL_SITUATION.ID_CLINICAL_PROTOCOL', 'CLINICAL_PROTOCOL.ID_CLINICAL_PROTOCOL')
                                          .leftJoin('NB_HISTOLOGY', 'NB_CLINICAL_SITUATION.ID_NB_HISTOLOGY', 'NB_HISTOLOGY.ID_NB_HISTOLOGY')
                                          .leftJoin('NB_PRIMARY_SITE', 'NB_CLINICAL_SITUATION.ID_NB_PRIMARY_SITE', 'NB_PRIMARY_SITE.ID_NB_PRIMARY_SITE')
                                          .where('ID_PATIENT',mysqlSubjId);

        logger.log('debug', query.toString());

        return query.then(function(rows) {

            if (_.isEmpty(rows)) return [null, null];

            let clinSit = rows[0];

            if (!clinSit.ID_NB_REG) return [null, null];

            let payload = {
                type: NB_CLINICAL_SITUATION_POSTGRES_ID,
                parentSubject: pgSubjId,
                metadata: {
                    italian_nb_registry_id: {value: clinSit.ID_NB_REG},
                    diagnosis_date: {value: formatDate(clinSit.DIAGNOSIS_DATE)},
                    diagnosis_age: { value: clinSit.DIAGNOSIS_AGE, unit: 'month'},
                    clinical_protocol: {value: clinSit.NAME_CLINICAL_PROTOCOL},
                    inss: {value: clinSit.INSS || null},
                    inrgss: {value: clinSit.INRGSS || null},
                    histology: {value: clinSit.DESCR_NB_HISTOLOGY},
                    primary_site: {value: clinSit.DESCR_NB_PRIMARY_SITE},
                    relapse: {value: clinSit.RELAPSE || null},
                    relapse_date: {value: formatDate(clinSit.RELAPSE_DATE)},
                    relapse_type: {value: clinSit.RELAPSE_TYPE || null},
                    last_follow_up_date: {value: formatDate(clinSit.LAST_FOLLOW_UP_DATE)},
                    clinical_follow_up_status: {value: clinSit.CLINICAL_FOLLOW_UP_STATUS || null},
                    ploidy: {value: clinSit.PLOIDY},
                    mycn_status: {value: clinSit.MYCN_STATUS || null},
                    event_overall: {value: clinSit.EVENT_OVERALL || 'N.D.'},
                    event_progfree: {value: clinSit.EVENT_PROGFREE || 'N.D.'},
                    survival_overall: {value: clinSit.SURVIVAL_OVERALL, unit: 'day'},
                    survival_progfree: {value: clinSit.SURVIVAL_PROGFREE, unit: 'day'}
                },
                date: formatDate(new Date())
            };

            logger.log('info', 'Creating new NB Clinical Situation: ');
            logger.log('debug', payload);
            return request.postAsync({
                uri: basePath + '/data',
                auth: {
                    bearer: connections.bearerToken
                },
                json: payload
            });

        })
        .spread(function(res, body) {

            if (res && res.statusCode !== CREATED) {
                logger.log('error', res && res.request && res.request.body);
                throw new Error("Migrator.createNBClinicalData: clinical data was not correctly created for MySQL subject code " + mysqlSubjId);
            }
            return true;

        });

    },

    /**
     * @method
     * @name migrateSamples
     * @description migrate primary samples
     */
    migrateSamples: function(mysqlSubjId) {

        let pgSubjId = this.subjectMap[mysqlSubjId];

        logger.log('info', "Migrator.migrateSamples - idSubject: " + pgSubjId);

        let knexPg = this.knexPg;
        let that = this;
        let samples;

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
                                         let primaryIds = _.pluck(samples, 'ID_SAMPLE');
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
                                             let tumourStatus = _.findWhere(tsRows, {ID_SAMPLE: sample.ID_SAMPLE});
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
        if (!sample.BIT_CODE) {
            logger.log('info',"Migrator.migrateSampleDerivatives - sample misses required info: " + sample.ID_SAMPLE);
            return;
        }
        // console.log("Migrator.createPrimary - idSubject: " + idSubj);
        let that = this;
        let knexPg = this.knexPg;
        let sampleType, sampleTypeName, idPrimarySample, ts;

        // change FAT to ADIPOUS TISSUE
        if (sample.DESCR_BIT_TISSUE_NAME === 'FAT') sample.DESCR_BIT_TISSUE_NAME = 'ADIPOUS TISSUE';

        let metadata = {
            arrival_date_mb: {value: formatDate(sample.BM_ARRIVAL_DATE)},
            sample_name: {value: sample.SAMPLE_NAME || null},
            sample_codification: {value: sample.DESCR_BIT_TISSUE_NAME || 'UNDETERMINED'},
            pathology: {value: sample.DESCR_BIT_HISTOPATHOLOGY || 'NONE'},
            city: { value: sample.CITY },
            hospital: { value: sample.DESCR_HOSP || null},
            unit: { value: sample.UNIT }
        };

        switch (sample.ID_SAMPLE_TYPE) {
        case 'TIS':
            metadata.arrival_code = {value: sample.ARRIVAL_CODE || null};
            metadata.arrival_date_pa = {value: formatDate(sample.AP_ARRIVAL_DATE) };
            metadata.size_x = {value: sample.SIZE_FIRST, unit: 'mm'};
            metadata.size_y = {value: sample.SIZE_SECOND, unit: 'mm'};
            metadata.size_z = {value: sample.SIZE_THIRD, unit: 'mm'};
            metadata.tumour_cellularity = {value: sample.CELLULARITY, unit: '%'};
            sampleTypeName = 'Tissue';
            break;
        case 'FLD':
            metadata.sampling_date = {value: formatDate(sample.AP_ARRIVAL_DATE) };
            metadata.quantity = {value: sample.QUANTITY, unit: 'ml'};
            sampleTypeName = 'Fluid';
        }

        // if the tumour status is not empty
        if (!_.isEmpty(tumourStatus)) {
            let jsonSchema = JSON.parse(tumourStatus.JSON_SCHEMA);
            metadata.tumour_status = {value: jsonSchema.body[0].content[0].instances[0].value};
        }
        else {
            try {
                ts = sample.NOTES.split(/  +/)[0].split(':')[1].trim();
            }
            catch(err) {
                logger.log('warn', err.message);
                ts = null;
            }
            // console.log(ts);
            metadata.tumour_status = {value: ts || null};
            if (allowedTumourStatuses.indexOf(ts) > -1) {
                metadata.tumour_status = {value: ts};
            }
            else if (ts === 'POST-CHEMIO' || ts === 'POST CHEMIO') {
                metadata.tumour_status = {value: 'POST-CHEMO'};
            }
            else {
                metadata.tumour_status = {value: null};
            }
        }

        // console.log("Migrator.createPrimary - metadata: " + metadata);
        return knexPg.select('id').from('data_type').where('name', sampleTypeName)

        .then(function(rows) {
            if (rows && rows.length) {
                sampleType = rows[0].id;
                return request.postAsync({
                    uri: basePath + '/sample',
                    auth: {
                        bearer: connections.bearerToken
                    },
                    json: {
                        type: sampleType,
                        biobank: 1,
                        biobankCode: sample.BIT_CODE,
                        donor: idSubj,
                        metadata: metadata
                    }
                });

            }
        })

        .spread(function(res, body) {
            // console.log("Migrator.createPrimary - response is: " + res.statusCode);
            if (res && res.statusCode !== CREATED) {
                logger.log('error', res && res.request && res.request.body);
                throw new Error("Migrator.createPrimary: sample was not correctly created for biobank code " + sample.BIT_CODE);
            }
            // idPrimarySample = ids[0];
            idPrimarySample = body.id;
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

        let that = this;

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
                    let metadata = {
                        arrival_date_mb: {value: formatDate(sample.BM_ARRIVAL_DATE) },
                        sample_name: {value: sample.SAMPLE_NAME || null},
                        sample_codification: {value: sample.DESCR_BIT_TISSUE_NAME || 'UNDETERMINED'},
                        pathology: {value: sample.DESCR_BIT_HISTOPATHOLOGY || 'NONE'},
                        city: { value: sample.CITY },
                        hospital: { value: sample.DESCR_HOSP || null},
                        unit: { value: sample.UNIT },
                        sampling_date: {value: formatDate(sample.AP_ARRIVAL_DATE)},
                        quantity: {value: sample.QUANTITY || 0, unit: 'ml'}
                    };

                    return request.postAsync({
                        uri: basePath + '/sample',
                        auth: {
                            bearer: connections.bearerToken
                        },
                        json: {
                            type: sampleType,
                            biobank: 1,
                            metadata: metadata,
                            biobankCode: sample.BIT_CODE,
                            donor: idSubj,
                            parentSample: that.sampleMap[mysqlSampleId]
                        }
                    })
                    .spread(function(res, body) {
                        if (res.statusCode !== CREATED) {
                            // console.log(res && res.request && res.request.body);
                            throw new Error("Migrator.migratePlasmaSamples - sample was not correctly created for sample " + sample.BIT_CODE);
                        }
                        return true;
                    });

                });

            });

    },

    /**
     * @method
     * @name migrateNucleicDerivatives
     * @description migrate DNA and RNA samples from MySQL to PostgreSQL (>= 9.4) database
     */
    migrateNucleicDerivatives: function(mysqlSampleId, idSubj) {
        let that = this;
        let metadata, derivatives;

        logger.log('info',"Migrator.migrateSampleDerivatives - here we are");

        return this.knexMysql.select('SAMPLE.ID_SAMPLE','BIT_CODE', 'ARRIVAL_CODE', 'ID_SAMPLE_TYPE', 'QUANTITY', 'EXTRACTION_DATE', 'CONCENTRATION')
        .from('SAMPLE')
        .whereIn('ID_SAMPLE_TYPE', ['DNA','RNA']).andWhere('ID_PARENT_SAMPLE', mysqlSampleId)

        .then(function(rows) {
            derivatives = rows;
            // console.log("Migrator.migrateNucleicDerivatives - got these samples: ");
            // console.log(derivatives);
            let derivativesId = _.pluck(rows, 'ID_SAMPLE');
            return that.knexMysql.select('JSON_SCHEMA', 'ID_FCOLL', 'ID_SAMPLE').from('DATA')
            .leftJoin('SAMPLE_COLLECTION', 'SAMPLE_COLLECTION.ID_SCOLL', 'DATA.ID_SCOLL')
            .leftJoin('SAMPLE_SCOLL', 'SAMPLE_SCOLL.ID_SCOLL', 'SAMPLE_COLLECTION.ID_SCOLL')
            .whereIn('DESCR_DATA', ['QUALITY CONTROL - DNA', 'QUALITY CONTROL - RNA'])
            .whereIn('ID_SAMPLE', derivativesId);
        })

        .then(function(qcRows) {
            let jsonSchema;
            // console.log("Migrator.migrateNucleicDerivatives - qcRows: ");
            // console.log(qcRows);
            return BluebirdPromise.each(derivatives, function(derivative) {
                if (!derivative.BIT_CODE || derivative.BIT_CODE === "NO" || !derivative.EXTRACTION_DATE) {
                    // console.log ("Migrator.migrateSampleDerivatives - sample misses required info: " + derivative.ID_SAMPLE);
                    // TODO log missing samples
                    return;
                }

                let quality, mappedDerivative;
                let qc = _.findWhere(qcRows, {'ID_SAMPLE': derivative.ID_SAMPLE}) || {};

                logger.log('info', "Migrator.migrateNucleicDerivatives - qc: ");
                // console.log(qc);
                if (derivative.QUANTITY === null || derivative.QUANTITY === undefined) {
                    logger.log('warn', "Migrator.migrateSampleDerivatives - sample " + derivative.ID_SAMPLE + " has no quantity information");
                }

                metadata = {
                    arrival_code: {value: derivative.ARRIVAL_CODE || null},
                    sampling_date: {value: formatDate(derivative.EXTRACTION_DATE) },
                    quantity: {value: derivative.QUANTITY || 0, unit: 'μg'},
                    concentration: {value: derivative.CONCENTRATION, unit: 'ng/μl'}
                };

                if (!_.isEmpty(qc)) {
                    jsonSchema = JSON.parse(qc.JSON_SCHEMA);
                    quality = allowedQualities.indexOf(jsonSchema.body[0].content[0].instances[0].value) > -1 ?
                        jsonSchema.body[0].content[0].instances[0].value : allowedQualities[allowedQualities.length-1];
                    metadata.quality = {value: quality};
                    metadata.kit_type = {value: jsonSchema.body[0].content[1].instances[0].value};
                    metadata._260_280 = {value: jsonSchema.body[0].content[2].instances[0].value};
                    metadata._260_230 = {value: jsonSchema.body[0].content[3].instances[0].value};
                }

                let payload =  {
                    type: that.dataTypeMap[derivative.ID_SAMPLE_TYPE],
                    biobank: 1,
                    biobankCode: derivative.BIT_CODE,
                    donor: idSubj,
                    parentSample: that.sampleMap[mysqlSampleId],
                    metadata: metadata
                };

                // retrieve data files associated to the QC
                return that.getAndFormatDataFiles(qc.ID_FCOLL)

                .then(function(files) {

                    payload.files = files;
                    logger.log('info', "Migrator.migrateNucleicDerivatives - inserting derivative sample: ");
                    logger.log('debug', payload);

                    return request.postAsync({
                        uri: basePath + '/sample',
                        auth: {
                            bearer: connections.bearerToken
                        },
                        json: payload
                    });
                })

                .spread(function(res, body) {
                    if (res.statusCode !== CREATED) {
                        logger.log('error',  res.statusCode);
                        logger.log('error', res && res.request && res.request.body);
                        throw new Error("Migrator.migrateSampleDerivatives - sample was not correctly created for biobank code " +  derivative.BIT_CODE);
                    }
                    // console.log(body);
                    return {
                        idSampleMysql: derivative.ID_SAMPLE,
                        idSamplePg: body.id,
                        type: body.type
                    };
                })

                .then(function(sampleObj) {

                    mappedDerivative = sampleObj;
                    logger.log('info', 'Migrator.migrateNucleicDerivatives - derivative migrated');
                    logger.log('info', sampleObj);

                    if (sampleObj.type === that.dataTypeMap.RNA) {
                        logger.log('info', "Migrator.migrateSampleDerivatives - sample is RNA migrating Microarrays...");
                        logger.log('info', sampleObj);
                        return that.migrateMicroarrays(sampleObj.idSampleMysql, sampleObj.idSamplePg, idSubj);
                    }

                })

                .then(function() {

                    logger.log('info', "Migrator.migrateSampleDerivatives - ready to migrate aliquot deliveries");
                    return that.migrateAliquotDelivery(mappedDerivative.idSampleMysql, mappedDerivative.idSamplePg, idSubj);

                })

                .then(function() {

                    logger.log('info', "Migrator.migrateSampleDerivatives - ready to migrate ALK Reports");
                    return that.migrateAlkReport(mappedDerivative.idSampleMysql, mappedDerivative.idSamplePg, idSubj);

                });

            });

        })

        .then(function(samples) {

            logger.log('info', 'Migrator.migrateNucleicDerivatives - done!');
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
        let that = this, idCghProcessed;
        let files = utils.getFilesInFolder(folder, ext);
        if (_.isEmpty(files)){
            logger.log('info', "Invalid or no files loaded");
            return BluebirdPromise.rejected("No Valid files loaded");
        }
        return BluebirdPromise.each(files, function(file, index) {
            console.log(file);
            let metadataBatch = utils.composeCGHMetadata(file);
            logger.log('info',"Migrator.migrateCGH - here we are");

            let queryPayload = {
                "queryArgs":{
                    "wantsSubject":true,
                    "dataType":4,
                    "model":"Sample",
                    "content":[{
                        "fieldName":"arrival_code",
                        "fieldType":"text",
                        "comparator":"=",
                        "fieldValue": metadataBatch.sampleCode
                    },{
                        "dataType":14,
                        "model":"Data",
                        "content":[{
                            "fieldName":"recipient",
                            "fieldType":"text",
                            "comparator":"=",
                            "fieldValue":"PEZZOLO"
                        }]
                    }]
                }
            };

            logger.log('info', queryPayload);

            /*
               return that.knexPg.raw(["SELECT s.id, s.metadata FROM sample s LEFT JOIN data_type d ON d.id = s.type",
               "WHERE s.metadata @> ? AND d.name = 'DNA'"].join(" "), [param])
               */
            return request.postAsync({
                uri: basePath + '/query/dataSearch',
                auth: {
                    bearer: connections.bearerToken
                },
                json: queryPayload
            })

            .spread(function(res, body) {
                if (res.statusCode !== OK || !body) {
                    logger.log('error',  res.statusCode);
                    logger.log('error', res && res.request && res.request.body);
                    logger.log('info', 'Skipping file: ' + file);
                    return;
                }
                logger.log('info', "Migrator.migrateCGH - sample query results: ");
                logger.log('info', body && body.data);
                if (!_.isArray(body.data) || _.isEmpty(body.data)) {
                    console.log("Migrator.migrateCGH: no sample found");
                    return;
                }
                let idSample = body.data[0].id, idSubj;

                return request.getAsync({
                    uri: basePath + '/sample/' + idSample,
                    auth: {
                        bearer: connections.bearerToken
                    }
                })

                .spread(function(res, body) {
                    logger.log('info', 'parent DNA found: ' + body) ;
                    body = JSON.parse(body);

                    idSubj = body.donor;
                    logger.log('debug', 'id parent subject: ' + idSubj);

                    // create CGH raw data instance
                    return request.postAsync({
                        uri: basePath + '/data',
                        auth: {
                            bearer: connections.bearerToken
                        },
                        json: {
                            type: 6, // CGH Raw type
                            metadata: {
                                platform: {value: 'Agilent'},
                                array: {value: '4x180K'}
                            },
                            parentSample: idSample,
                            parentSubject: idSubj
                        }
                    });
                })
                .spread(function(res, body) {
                    if (res.statusCode !== CREATED) {
                        logger.log('error', "CGH-RAW was not correctly created");
                        // logger.log('error', res.message);
                        throw new Error(res.message);
                    }
                    logger.log('info', metadataBatch.acghProcessed);

                    return request.postAsync({
                        uri: basePath + '/data',
                        auth: {
                            bearer: connections.bearerToken
                        },
                        json: {
                            type: 7, // CGH Processed type
                            metadata: metadataBatch.acghProcessed,
                            parentSubject: idSubj,
                            parentData: body.id
                        }
                    });
                })
                .spread(function(res, body) {
                    if (res.statusCode !== CREATED) {
                        logger.log("error", "CGH-Processed  was not correctly created");
                        // logger.log("error", res.message);
                        throw new Error(res.message);
                    }
                    logger.log('debug', 'Created CGH-Processed: ' + body);
                    idCghProcessed = body.id;
                    console.log(idCghProcessed);
                    return request.postAsync({
                        uri: basePath + '/data',
                        auth: {
                            bearer: connections.bearerToken
                        },
                        json: {
                            type: 18, // Genomic Profile type
                            metadata: metadataBatch.genProfile,
                            parentSubject: idSubj,
                            parentData: idCghProcessed
                        }
                    });
                })
                  .spread(function(res, body) {
                      if (res.statusCode !== CREATED) {
                          logger.log("error", "Genomic Profile  was not correctly created");
                          // logger.log("error", res.message);
                          throw new Error(res.message);
                      }
                      return BluebirdPromise.each(metadataBatch.cnletr, function(cnv) {
                          return request.postAsync({
                              uri: basePath + '/data',
                              auth: {
                                  bearer: connections.bearerToken
                              },
                              json: {
                                  type: 8,
                                  metadata: cnv,
                                  parentSubject: idSubj,
                                  parentData: idCghProcessed
                              }
                          });
                      });
                  })
                .then(function(res) {
                    logger.log('info',"Migrator.migrateCGH -  done for sample:" + metadataBatch.sampleCode);
                });
                /*
                .catch(function(err) {
                    logger.log("error","Migrator.migrateCGH - Exception caught while creating CGH data: " + err.message);
                    // TODO Rollback (??)
                }); */
            });

        })
        .then(function() {
            logger.log("info","All CGH data were stored correctly");
            return true;
        })
        .catch(function(error) {
            logger.log('error', "Migrator.migrateCGH - Got some errors while storing CGH:" + error.message);
            return false;
        });
    },

    importCBInfo: function(folder, ext) {

        let files = utils.getFilesInFolder(folder, ext);

        if (_.isEmpty(files)){
            logger.log('info', "Invalid or no files loaded");
            return BluebirdPromise.rejected("No Valid files loaded");
        }

        let workbook = xlsx.readFile(files[0]);
        let worksheet = workbook.Sheets[workbook.SheetNames[0]];
        let range = xlsx.utils.decode_range(worksheet['!ref']);

        //Create the json file from xlsx file
        let patients = xlsx.utils.sheet_to_json(worksheet);
        
        if(!patients[0]['Cod_RINB']){
            return BluebirdPromise.rejected("No Valid files loaded");
        }

        let updated = 0, created = 0;
        let name, surname, birthDate, idSubject, metadataCBInfo, subjects, cbInfos;

        let queryPayload = {
            "queryArgs":{
                "wantsSubject":true,
                "wantsPersonalInfo":true,
                "dataType":1,
                "model":"Subject",
                "content":[{
                    "personalDetails":true
                },
                {"specializedQuery":"Subject"},
                {"specializedQuery":"Subject"}
                        ]}
        };
        return request.postAsync({
            uri: basePath + '/query/dataSearch',
            auth: {
                bearer: connections.bearerToken
            },
            json: queryPayload
        })
        .spread(function(res, body) {

            if (res.statusCode !== OK || !body) {
                logger.log('error',  res.statusCode);
                logger.log('error', res && res.request && res.request.body);
                return BluebirdPromise.rejected("Error loading Data");
            }

            subjects = body && body.data;

            return request.postAsync({
                uri: basePath + '/query/dataSearch',
                auth: { bearer: connections.bearerToken },
                json: {
                    "queryArgs": {
                        "wantsPersonalInfo": true,
                        "wantsSubject": true,
                        "dataType": 16,
                        "model": "Data"
                    }
                }
            })
        .spread(function(res, body) {

            if (res.statusCode !== OK || !body) {
                logger.log('error',  res.statusCode);
                logger.log('error', res && res.request && res.request.body);
                return BluebirdPromise.rejected("Error loading Subjects");
            }

            cbInfos = body && body.data;

            return BluebirdPromise.each(patients, patient => {

                let cbInfo = _.find(cbInfos, function( obj ) {
                    return obj.metadata['italian_nb_registry_id'].value == patient['Cod_RINB'];
                });

                if(cbInfo){
                    let metadataCBInfo = utils.composeCBInfoMetadata(patient);
                    let idCbInfo = cbInfo.id;
                    updated = updated + 1;
                    logger.log('info',"Migrator.updatingCBInfo - patient: " + patient.Nome + " " + patient.Cognome + " cbInfo to be updated. " + cbInfo.id + " " + updated);
                    return request.putAsync({
                        uri: basePath + '/data/' + idCbInfo,
                        auth: {
                            bearer: connections.bearerToken
                        },
                        json: {
                            id: idCbInfo,
                            date: moment().format("YYYY-MM-DD"),
                            type: cbInfo.type,
                            metadata: metadataCBInfo
                        }
                    });
                }
                else{

                    let subject = _.find(subjects, function( obj ) {
                        return (obj['surname'] == patient['Cognome'].toUpperCase()
                    && obj['given_name'] == patient['Nome'].toUpperCase()
                    && moment(obj['birth_date']).format("DD/MM/YYYY") == patient['Data di nascita']) ||
                    (obj['surname'] == patient['Cognome'].toUpperCase()
                    && moment(obj['birth_date']).format("DD/MM/YYYY") == patient['Data di nascita'])||
                    (obj['given_name'] == patient['Nome'].toUpperCase()
                    && moment(obj['birth_date']).format("DD/MM/YYYY") == patient['Data di nascita']);
                    });

                    if(subject){

                        let metadataCBInfo = utils.composeCBInfoMetadata(patient);
                        let idSubject = subject.id;
                        created = created + 1;
                        logger.log('info', "Migrator.updatingCBInfo - patient: " + patient.Nome + " " + patient.Cognome + " cbInfo to be created. " + created);

                        return request.postAsync({
                            uri: basePath + '/data',
                            auth: {
                                bearer: connections.bearerToken
                            },
                            json: {
                                date: moment().format("YYYY-MM-DD"),
                                type: 16,
                                parentSubject: idSubject,
                                metadata: metadataCBInfo
                            }
                        });
                    }
                    else{
                        logger.log('info',"Migrator.updatingCBInfo - patient: " + patient.Nome + " " + patient.Cognome + " with code: " + patient.Cod_RINB + "is not present into DB. ");
                        return;
                    }
                }

            });

        }).then(function() {
            logger.log('info', "Migrator.updatingCBInfo - " + created + " cbInfo created and " + updated + " updated correctly.");
            return true;
        }).catch(function(error) {
            logger.log('info', "Migrator.updatingCBInfo - Got some errors while updating CBInfo:" + error.message);
            return false;
        });
        })
        .then(function() {
            logger.log('info', "Migrator.updatingCBInfo - All went fine!");
            return true;
        })
        .catch(function(error) {
            logger.log('info', "Migrator.updatingCBInfo - Got some errors while updating CBInfo:" + error.message);
            return false;
        });

    },

    /**
     * @method
     * @name getAndFormatDataFiles
     * @description retrieve associated data files from the old database and format them to be put on the new one
     * @param{integer} idFileCollMysql
     */
    getAndFormatDataFiles: function(idFileCollMysql) {

        // retrieve data files associated to the QC
        return this.knexMysql.select('URI').from('FILE_XTENS')
        .where('ID_FCOLL', idFileCollMysql).andWhere('FILE_TYPE', "DATA")

        .then(function(files) {
            if (!_.isEmpty(files)) {
                logger.log('info', "Migrator.migrateSampleDerivatives - found files");
                logger.log('info', files);
                files = _.map (files, file => {
                    return _.mapKeys(file, (value, key) => {
                        return key.toLowerCase();
                    });
                });
            }
            return files;
        });

    },

    /**
     * @method
     * @name migrateMicroarrays
     * @description migrate Microarray Data from old MySQL database to new PostgreSQL database
     * @param{integer} idSampleMysql - the sample ID on MySQL
     * @return{BluebirdPromise} a Bluebird promise
     */
    migrateMicroarrays: function(idSampleMysql, idSamplePg, idSubjPg) {

        let that = this, rawId, mas5Id;
        logger.log('info',"Migrator.migrateMicroarrays: here we are");

        let query = this.knexMysql.select('ID_DATA', 'ID_SAMPLE', 'ID_FCOLL', 'DESCR_DATA', 'JSON_SCHEMA')
        .from('DATA')
        .leftJoin('SAMPLE_COLLECTION', 'SAMPLE_COLLECTION.ID_SCOLL', 'DATA.ID_SCOLL')
        .leftJoin('SAMPLE_SCOLL', 'SAMPLE_SCOLL.ID_SCOLL', 'SAMPLE_COLLECTION.ID_SCOLL')
        .whereIn('DESCR_DATA', [MICROARRAY_RAW, MICROARRAY_MAS5, MICROARRAY_NB])
        .andWhere('ID_SAMPLE', idSampleMysql);

        logger.log('debug', query.toString());

        return query.then(function(microarrayData) {

            let logOut = _.map(_.cloneDeep(microarrayData), datum => {
                return _.omit(datum, 'JSON_SCHEMA');
            });
            logger.log('info', logOut) ;

            if (_.isEmpty(microarrayData)) return;
            else logger.log('info',"Migrator.migrateMicroarrays: Microarray data found!!");

            let microarrayRaw = _.findWhere(microarrayData, {'DESCR_DATA': MICROARRAY_RAW});
            let microarrayMas5 = _.findWhere(microarrayData, {DESCR_DATA: MICROARRAY_MAS5});

            logger.log('debug', 'Microarray Raw:' + microarrayRaw);

            if (!microarrayRaw) {
                return;
            }
            let jsonSchema = JSON.parse(microarrayRaw.JSON_SCHEMA);

            return that.getAndFormatDataFiles(microarrayRaw.ID_FCOLL)

            .then(function(files) {

                return request.postAsync({
                    uri: basePath + '/data',
                    auth: {
                        bearer: connections.bearerToken
                    },
                    json: {
                        type: 11, // Microarray RAW
                        metadata: {
                            platform: {value: jsonSchema.body[0].content[0].instances[0].value}
                        },
                        parentSubject: idSubjPg,
                        parentSample: idSamplePg,
                        files: files
                    }
                });

            })

            .spread(function(res, createdRaw) {

                logger.log('debug', 'Migrator.migrateMicroarrays: created new raw: ' + createdRaw);

                rawId = createdRaw.id;
                if (!microarrayMas5) return;
                return that.getAndFormatDataFiles(microarrayMas5.ID_FCOLL);
            })

            .then(function(files) {

                if (!microarrayMas5) return;

                jsonSchema = JSON.parse(microarrayMas5.JSON_SCHEMA);

                return request.postAsync({
                    uri: basePath + '/data',
                    auth: {
                        bearer: connections.bearerToken
                    },
                    json: {
                        type: 12, // Microarray MAS5
                        metadata: {
                            normalization_details: {value: jsonSchema.body[0].content[0].instances[0].value}
                        },
                        parentSubject: idSubjPg,
                        parentData: rawId,
                        files: files
                    }
                });
            })

            .spread(function(res, createdMas5) {
                logger.log('debug', 'Migrator.migrateMicroarrays: created new mas5: ' + createdMas5);

                let microarrayReport = _.findWhere(microarrayData, {DESCR_DATA: MICROARRAY_NB});
                if (!microarrayReport) {
                    return;
                }
                jsonSchema = JSON.parse(microarrayReport.JSON_SCHEMA);

                let payload = {
                    type: 13, // Microarray NB Report
                    metadata: {
                        unit: { value: jsonSchema.body[0].content[0].instances[0].value },
                        hypoxic_profile_code: { value: jsonSchema.body[1].content[0].instances[0].value },
                        hypoxic_profile: { value: mapReportValue(jsonSchema.body[1].content[1].instances[0].value) },
                        _59_gene_profile_code: { value: jsonSchema.body[2].content[0].instances[0].value },
                        _59_gene_profile: { value: mapReportValue(jsonSchema.body[2].content[1].instances[0].value) },
                        prognostic_genes_code: { value: jsonSchema.body[3].content[0].instances[0].value },
                        alk: { value: mapReportValue(jsonSchema.body[3].content[1].instances[0].value) },
                        cd44: { value: mapReportValue(jsonSchema.body[3].content[2].instances[0].value) },
                        fam49a: { value: mapReportValue(jsonSchema.body[3].content[3].instances[0].value) },
                        fyn: { value: mapReportValue(jsonSchema.body[3].content[4].instances[0].value) },
                        mycn: { value: mapReportValue(jsonSchema.body[3].content[5].instances[0].value) },
                        trka: { value: mapReportValue(jsonSchema.body[3].content[6].instances[0].value) },
                        trkb: { value: mapReportValue(jsonSchema.body[3].content[7].instances[0].value) },
                        trkc: { value: mapReportValue(jsonSchema.body[3].content[8].instances[0].value) }
                    },
                    parentSubject: idSubjPg,
                    parentData: createdMas5.id
                };
                logger.log('info',"Migrator.migrateMicroarrays: payload for Microarray report is: ");
                logger.log('info',payload);

                return request.postAsync({
                    uri: basePath + '/data',
                    auth: {
                        bearer: connections.bearerToken
                    },
                    json: payload
                });
            });

        });

    },

    /**
     * @method
     * @name migrateAliquotDelivery
     * @description migrates all the aliquot shipping info
     * @param{integer} idSampleMysql - sample ID on MySQL
     * @param{integer} idSamplePg - sample ID on PostgreSQL
     * @param{integer} idSubjPg - subject ID on PostgreSQL
     */
    migrateAliquotDelivery: function(idSampleMysql, idSamplePg, idSubjPg) {

        let jsonSchema;
        let query = this.knexMysql.select('ID_DATA', 'ID_SAMPLE', 'ID_FCOLL', 'DESCR_DATA', 'JSON_SCHEMA')
        .from('DATA')
        .leftJoin('SAMPLE_COLLECTION', 'SAMPLE_COLLECTION.ID_SCOLL', 'DATA.ID_SCOLL')
        .leftJoin('SAMPLE_SCOLL', 'SAMPLE_SCOLL.ID_SCOLL', 'SAMPLE_COLLECTION.ID_SCOLL')
        .whereIn('DESCR_DATA', [ALIQUOT_DELIVERY])
        .andWhere('ID_SAMPLE', idSampleMysql);

        logger.log('debug', query.toString());

        return query.then(function(aliquotDeliveryData) {
            return BluebirdPromise.map(aliquotDeliveryData, function(ad) {

                jsonSchema = JSON.parse(ad.JSON_SCHEMA);

                return request.postAsync({
                    uri: basePath + '/data',
                    auth: {
                        bearer: connections.bearerToken
                    },
                    json: {
                        type: 14, // aliquot delivery
                        metadata: {
                            sample_type: { value: jsonSchema.body[0].content[0].instances[0].value },
                            shipped_quantity: {
                                value: jsonSchema.body[0].content[1].instances[0].value,
                                unit: 'µg'
                            },
                            recipient: { value: jsonSchema.body[0].content[2].instances[0].value }
                        },
                        parentSubject: idSubjPg,
                        parentSample: idSamplePg
                    }
                });

            })

            .then(function(inserted) {
                logger.log('info', 'Migrator.migrateAliquotDelivery - done!');
                logger.log('info', 'Inserted ' + inserted.length + 'record(s)');
                return true;
            });
        });


    },

    /**
     * @method
     * @name migrateAlkReport
     * @description migrates all the ALK Reports
     * @param{integer} idSampleMysql - sample ID on MySQL
     * @param{integer} idSamplePg - sample ID on PostgreSQL
     * @param{integer} idSubjPg - subject ID on PostgreSQL
     *
     */
    migrateAlkReport: function(idSampleMysql, idSamplePg, idSubjPg) {

        let jsonSchema;
        let query = this.knexMysql.select('ID_DATA', 'ID_SAMPLE', 'ID_FCOLL', 'DESCR_DATA', 'JSON_SCHEMA')
        .from('DATA')
        .leftJoin('SAMPLE_COLLECTION', 'SAMPLE_COLLECTION.ID_SCOLL', 'DATA.ID_SCOLL')
        .leftJoin('SAMPLE_SCOLL', 'SAMPLE_SCOLL.ID_SCOLL', 'SAMPLE_COLLECTION.ID_SCOLL')
        .where('DESCR_DATA', ALK_MUTATION).andWhere('ID_SAMPLE', idSampleMysql);

        logger.log('debug', query.toString());

        return query.then(function(alkData) {

            if (_.isEmpty(alkData)) {
                return;
            }

            return BluebirdPromise.each(alkData, function(alk) {

                jsonSchema = JSON.parse(alk.JSON_SCHEMA);

                return request.postAsync({
                    uri: basePath + '/data',
                    auth: {
                        bearer: connections.bearerToken
                    },
                    json: {
                        type: 15, // ALK mutation
                        metadata: {
                            exon_20: { values:  _.pluck(jsonSchema.body[0].content[0].content[0].instances, 'value') },
                            exon_21: { values: _.pluck(jsonSchema.body[0].content[1].content[0].instances, 'value') },
                            exon_22: { values: _.pluck(jsonSchema.body[0].content[2].content[0].instances, 'value') },
                            exon_23: { values: _.pluck(jsonSchema.body[0].content[3].content[0].instances, 'value') },
                            exon_24: { values: _.pluck(jsonSchema.body[0].content[4].content[0].instances, 'value') },
                            exon_25: { values: _.pluck(jsonSchema.body[0].content[5].content[0].instances, 'value') },
                            result: { value: jsonSchema.body[1].content[0].instances[0].value},
                            germline_mutation: { value: jsonSchema.body[2].content[0].instances[0].value }
                        },
                        parentSubject: idSubjPg,
                        parentSample: idSamplePg
                    }
                })

                .spread(function(res, body) {
                    if (res.statusCode !== CREATED) {
                        logger.log('error',  'Migrator.migrateAlkReport - ALK Mutation Report not correctly created');
                    }
                });

            })

            .then(function(inserted) {
                logger.log('info', 'Migrator.migrateAlkReport - done!');
                logger.log('info', 'Inserted ' + inserted.length + 'record(s)');
                return true;
            });

        });


    }


};

module.exports = Migrator;
