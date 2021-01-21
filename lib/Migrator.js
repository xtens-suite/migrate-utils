/**
 * @author Massimiliano Izzo
 * @description main tool for migration
 */
/* jshint node:true */
/* jshint esnext: true */
"use strict";

let _ = require("lodash");
let http = require("http");
let connections = require('../config/connections.js');
const basePath = connections.basePath;
let BluebirdPromise = require('bluebird');
let utils = require("./utils.js");
let DaemonService = require("./DaemonService.js");
let allowedTumourStatuses = ["ONSET", "POST-CHEMO", "RELAPSE", "POST-CHEMO RELAPSE"];
let allowedQualities = ["GOOD", "AVERAGE", "POOR", "N.D."];
let request = BluebirdPromise.promisify(require("request"));
var loggerGen = require('./../logger.js');
const logger = loggerGen();

let moment = require('moment-timezone');
let xlsx = require("xlsx");

const co = require('co');

const CNV_HEADER_FIRST_CELL_CONTENT = 'AberrationNo';
const OK = 200;
const CREATED = 201;
const MICROARRAY_RAW = 'MICROARRAY - RAW';
const MICROARRAY_MAS5 = 'MICROARRAY - MAS5';
const MICROARRAY_NB = 'MICROARRAY - NB';
const ALIQUOT_DELIVERY = 'ALIQUOT DELIVERY';
const ALK_MUTATION = 'ALK - MUTATION';
const DEFAULT_LOCATION_TO_SPLIT = '../xtens-app/assets/dataFiles/tmp/';

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

const coroutines = {

    migrateCGH: BluebirdPromise.coroutine(function* (folder, ext, processInfo) {

        let objInfo = JSON.parse(processInfo.argv[2]);
        let data = [];
        let randomFolder = objInfo.folder;
        folder = randomFolder ? folder + '/' + randomFolder : folder;
        let files = utils.getFilesInFolder(folder, ext);
        if (files.length === 0) {
            let errorString = "Invalid or no files loaded";
            let daemon = yield DaemonService.InitializeDeamon(" ", {}, objInfo.executor, processInfo, objInfo.bearerToken);
            daemon = yield DaemonService.ErrorDeamon(daemon, errorString, objInfo.bearerToken);
            logger.log('info', "Invalid or no files loaded");
            return BluebirdPromise.rejected(errorString);
        }
        let daemons = [], filesTobeProcessed = [];
        yield BluebirdPromise.each(files, co.wrap(function* (file, index) {
            let filename = file.split(folder + '/')[1];
            let dmn = yield DaemonService.InitializeDeamon(filename, {}, objInfo.executor, processInfo, objInfo.bearerToken);
            if (dmn && dmn.status === 'initializing') {
                filesTobeProcessed.push(file);
                daemons.push(dmn);
            }
        }));

        yield BluebirdPromise.each(files, co.wrap(function* (file, daemonIndex) {
            let notInserted = [], created = 0;
            let fileName = file.split(folder + '/')[1];
            logger.log('info', "Migrator.migrateCGH - file: ", file);
            let metadataBatch = utils.composeCGHMetadata(file);

            daemons[daemonIndex].info.totalRows = metadataBatch.cnletr.length;
            daemons[daemonIndex].info.processedRows = 0;
            daemons[daemonIndex] = yield DaemonService.UpdateDeamon(daemons[daemonIndex], objInfo.bearerToken);

            logger.log('info', "Migrator.migrateCGH - here we are");

            let queryPayload = {
                "queryArgs": {
                    "wantsSubject": true,
                    "dataType": 4,
                    "model": "Sample",
                    "content": [{
                        "fieldName": "arrival_code",
                        "fieldType": "text",
                        "comparator": "=",
                        "fieldValue": metadataBatch.sampleCode
                    }, {
                        "dataType": 14,
                        "model": "Data",
                        "content": [{
                            "fieldName": "recipient",
                            "fieldType": "text",
                            "comparator": "=",
                            "fieldValue": "OGNIBENE"
                        }]
                    }]
                }
            };

            let [res, body] = yield request.postAsync({
                uri: basePath + '/query/dataSearch',
                auth: { bearer: objInfo.bearerToken },
                json: queryPayload
            });
            if (res.statusCode !== OK || !body) {
                logger.log('error', res.statusCode);
                logger.log('error', res && res.request && res.request.body);
                logger.log('info', 'Skipping file: ' + file);
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], res.body, objInfo.bearerToken);

                return;
            }
            if (!body.data[0]) {
                let errorString = "Migrator.migrateCGH: no sample with aliquot found";
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                return;
            }

            let idSample = body.data[0].id, idSubj;

            let [resSample, bodyS] = yield request.getAsync({
                uri: basePath + '/sample/' + idSample + '?populate=donor',
                auth: {
                    bearer: objInfo.bearerToken
                }
            });
            if (resSample.statusCode !== OK || !bodyS) {
                logger.log('error', "Error Getting sample");
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], resSample.body, objInfo.bearerToken);
                return;
            }
            let bodySample = JSON.parse(bodyS);
            if (!bodySample) {
                let errorString = "Migrator.migrateCGH: no DNA found";
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                return;
            }

            // CONTROLLO CHE IL FILE NON SIA GIÀ STATO INSERITO
            let queryPayload2 = {
                "queryArgs": {
                    "wantsSubject": true,
                    "dataType": 4,
                    "model": "Sample",
                    "content": [{
                        "fieldName": "arrival_code",
                        "fieldType": "text",
                        "comparator": "=",
                        "fieldValue": metadataBatch.sampleCode
                    }, {
                        "dataType": 6,
                        "model": "Data",
                        "content": [{
                            "fieldName": "platform",
                            "fieldType": "text",
                            "comparator": "=",
                            "fieldValue": "Agilent"
                        }]
                    }]
                }
            };

            let [res2, body2] = yield request.postAsync({
                uri: basePath + '/query/dataSearch',
                auth: { bearer: objInfo.bearerToken },
                json: queryPayload2
            });
            if (res2.statusCode !== OK || !body2) {
                logger.log('error', res2.statusCode);
                logger.log('error', res2 && res2.request && res2.request.body);
                logger.log('info', 'Skipping file: ' + file);
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], res2.body, objInfo.bearerToken);

                return;
            }
            if (body2.data[0]) {
                let errorString = fileName + " has already been inserted";
                logger.log('info', fileName + " has already been inserted");
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                return;
            }
            // let filesAlreadyInserted = bodySample.metadata && bodySample.metadata.data_files_inserted ? bodySample.metadata.data_files_inserted.values : [];
            // let alreadyInserted = _.find(filesAlreadyInserted, function (file) {
            //    return file === fileName;
            // });
            // if (alreadyInserted) {
            //    let errorString = fileName + " has already been inserted";
            //   logger.log('info', fileName + " has already been inserted");
            //    daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
            //    return;
            // }

            logger.log('info', 'parent DNA found: ' + bodySample);
            idSubj = bodySample.donor[0].id;
            logger.log('info', 'id parent subject: ' + idSubj);

            let [resCGHRaw, bodyCGHRaw] = yield request.postAsync({
                uri: basePath + '/data',
                auth: {
                    bearer: objInfo.bearerToken
                },
                json: {
                    type: objInfo.dataTypeId, // CGH Raw type
                    owner: objInfo.owner,
                    metadata: {
                        platform: { value: 'Agilent' },
                        array: { value: '4x180K' }
                    },
                    parentSample: [idSample],
                    parentSubject: [idSubj]
                }
            });

            if (resCGHRaw.statusCode !== CREATED) {
                let errorString = "CGH-RAW was not correctly created. " + bodyCGHRaw.error.message.details[0].message;
                logger.log('info', errorString);
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                return;
            }
            let idCghRaw = bodyCGHRaw.id;
            // logger.log('info', metadataBatch.acghProcessed);

            let [resCGHProc, bodyCGHProc] = yield request.postAsync({
                uri: basePath + '/data',
                auth: {
                    bearer: objInfo.bearerToken
                },
                json: {
                    type: 7, // CGH Processed type
                    owner: objInfo.owner,
                    metadata: metadataBatch.acghProcessed,
                    parentSubject: [idSubj],
                    parentData: [idCghRaw]
                }
            });

            if (resCGHProc.statusCode !== CREATED) {
                let errorString = "CGH-PROCESSED was not correctly created. " + bodyCGHProc.error.message.details[0].message;
                logger.log('info', errorString);
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                //rollback
                yield request.deleteAsync({
                    uri: basePath + '/data/' + idCghRaw,
                    auth: {
                        bearer: objInfo.bearerToken
                    }
                });
                logger.log('info', 'Rollback Operation');

                return;
            }

            logger.log('info', 'Created CGH-Processed: ' + bodyCGHProc.id);
            let idCghProcessed = bodyCGHProc.id;

            let [resGenProf, bodyGenProf] = yield request.postAsync({
                uri: basePath + '/data',
                auth: {
                    bearer: objInfo.bearerToken
                },
                json: {
                    type: 18, // Genomic Profile type
                    owner: objInfo.owner,
                    metadata: metadataBatch.genProfile,
                    parentSubject: [idSubj],
                    parentData: [idCghProcessed]
                }
            });

            if (resGenProf.statusCode !== CREATED) {
                let errorString = "GENOMIC PROFILE was not correctly created. " + bodyGenProf.error.message.details[0].message;
                logger.log('info', errorString);
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                //rollback
                yield request.deleteAsync({
                    uri: basePath + '/data/' + idCghRaw,
                    auth: {
                        bearer: objInfo.bearerToken
                    }
                });
                logger.log('info', 'Rollback Operation');

                return;
            }

            logger.log('info', 'Created GENOMIC PROFILE: ' + bodyGenProf.id);


            yield BluebirdPromise.each(metadataBatch.cnletr, co.wrap(function* (cnv, index) {

                let [res, body] = yield request.postAsync({
                    uri: basePath + '/data',
                    auth: {
                        bearer: objInfo.bearerToken
                    },
                    json: {
                        type: 8, //CNV
                        owner: objInfo.owner,
                        metadata: cnv,
                        parentSubject: [idSubj],
                        parentData: [idCghProcessed]
                    }
                });

                if (res.statusCode !== CREATED) {
                    notInserted.push({ index: index, data: cnv, error: "Error on cnv creation" });
                }
                else {
                    created = created + 1;
                    daemons[daemonIndex].info.processedRows = created;
                    daemons[daemonIndex] = yield DaemonService.UpdateDeamon(daemons[daemonIndex], objInfo.bearerToken);
                }
            }));
            //AGGIORNO IL CAMPIONE AGGIUNGENDO IL NOME DEL FILE APPENA CARICATO
            // filesAlreadyInserted.push(fileName);
            // if (bodySample.metadata.data_files_inserted) {
            //     bodySample.metadata.data_files_inserted.values = filesAlreadyInserted;
            // }
            // else {
            //     bodySample.metadata.data_files_inserted = {
            //         "loop": "Data Files",
            //         "group": "Inner attributes",
            //         "values": filesAlreadyInserted
            //     };
            // }
            bodySample.donor = _.map(bodySample.donor, 'id');
            let [resSampleUp, bodySampleUp] = yield request.putAsync({
                uri: basePath + '/sample/' + bodySample.id,
                auth: { bearer: objInfo.bearerToken },
                json: bodySample
            });

            logger.log('info', "Migrator.migrateCGH -  done for sample:" + metadataBatch.sampleCode);
            daemons[daemonIndex].info.processedRows = created;
            daemons[daemonIndex].info.notProcessedRows = notInserted;
            daemons[daemonIndex] = yield DaemonService.SuccessDeamon(daemons[daemonIndex], objInfo.bearerToken);
            return;
        }));


        logger.log("info", "All CGH files were stored correctly");
        return;
    }),

    migrateVCF: BluebirdPromise.coroutine(function* (folder, ext, processInfo, knexAnno) {
        // var start = new Date();
        const vcf = require('bionode-vcf');

        let objInfo = JSON.parse(processInfo.argv[2]);
        // let fileNames = [];
        let randomFolder = objInfo.folder;
        folder = randomFolder ? folder + '/' + randomFolder : folder;
        let files = utils.getFilesInFolder(folder, ext);
        if (files.length === 0) {
            let errorString = "Invalid or no files loaded";
            let daemon = yield DaemonService.InitializeDeamon(" ", {}, objInfo.executor, processInfo, objInfo.bearerToken);
            daemon = yield DaemonService.ErrorDeamon(daemon, errorString, objInfo.bearerToken);
            logger.log('info', "Invalid or no files loaded");
            return BluebirdPromise.rejected(errorString);
        }
        let daemons = [], filesTobeProcessed = [];
        yield BluebirdPromise.each(files, co.wrap(function* (file, index) {
            let filename = file.split(folder + '/')[1];
            let dmn = yield DaemonService.InitializeDeamon(filename, {}, objInfo.executor, processInfo, objInfo.bearerToken);
            if (dmn && dmn.status === 'initializing') {
                filesTobeProcessed.push(file);
                daemons.push(dmn);
            }
        }));

        yield BluebirdPromise.each(filesTobeProcessed, co.wrap(function* (file, daemonIndex) {
            let notInserted = [], created = 0;
            let subject, tissue, rowsFilesReady = [], filesAlreadyInserted = [], fileName, machine, mchn, capture;

            //INSERT BY PATIENT
            if (objInfo.vcfData && objInfo.vcfData.subjectId && objInfo.vcfData.sampleId && objInfo.vcfData.sampleType && objInfo.vcfData.machine && objInfo.vcfData.capture) {
                fileName = file.split(folder + '/')[1];
                subject = { id: objInfo.vcfData.subjectId };
                tissue = { id: objInfo.vcfData.sampleId };
                mchn = objInfo.vcfData.machine;
                capture = objInfo.vcfData.capture;
                switch (mchn) {
                    case "ILLUMINA":
                        machine = "ILL";
                        break;
                    case "ION TORRENT":
                        machine = "ION";
                        break;
                    default:
                        machine = null;
                }
                // ricerca soggetto trmite find ( da sviluppare la scelta tra find con e senza paginazione)
                let [resSubjects, bodySubjects] = yield request.getAsync({
                    uri: basePath + '/subject/' + subject.id,
                    auth: { bearer: objInfo.bearerToken }
                    // json: queryPayload
                });
                let bodyParsed = JSON.parse(bodySubjects);

                // SE SOGGETTO NON ESISTE ESCO DALLA PROCEDURA
                if (!bodyParsed || !bodyParsed.id) {
                    let errorString = "Subject with code " + codePatient + " not found";
                    logger.log('info', errorString);
                    daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                    return BluebirdPromise.rejected("Not found subject with code: " + codePatient);
                }
                subject = bodyParsed;

                // CONTROLLO CHE IL FILE NON SIA GIÀ STATO INSERITO
                filesAlreadyInserted = subject.metadata && subject.metadata.data_files_inserted ? subject.metadata.data_files_inserted.values : [];
                let alreadyInserted = _.find(filesAlreadyInserted, function (file) {
                    return file === fileName;
                });
                if (alreadyInserted) {
                    let errorString = fileName + " has already been inserted";
                    logger.log('info', fileName + " has already been inserted");
                    daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                    return;
                }
                //INSERT BULK BY FILENAME
            } else {


                let ValidationResult = utils.parseFileName(file, folder, 'vcf');
                if (ValidationResult.error) {
                    let errorString = "The filename is not properly formatted. Correct the filename and then retry. (es. AA-0001_F0001_SW-01_ILL_PANEL1.vcf)";
                    logger.log('error', errorString);
                    daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                    return;
                }
                fileName = ValidationResult.fileName;
                let codePatient = ValidationResult.codePatient,
                    // idFamily = ValidationResult.idFamily,
                    // tissueID = ValidationResult.tissueID,
                    tissueType = ValidationResult.tissueType,
                    tissueCode = ValidationResult.tissueCode;
                machine = ValidationResult.machine;
                capture = ValidationResult.capture;

                // CERCO IL SOGGETTO
                let queryPayload = {
                    "isStream": false,
                    "queryArgs": {
                        "wantsSubject": true,
                        "wantsPersonalInfo": true,
                        "dataType": objInfo.parentSubjectDtId,
                        "model": "Subject",
                        "content": [{
                            "personalDetails": true
                        },
                        {
                            "specializedQuery": "Subject",
                            "code": codePatient,
                            "codeComparator": "LIKE"
                        },
                        { "specializedQuery": "Subject" }
                        ]
                    }
                };

                let [resSubjects, bodySubjects] = yield request.postAsync({
                    uri: basePath + '/query/dataSearch',
                    auth: { bearer: objInfo.bearerToken },
                    json: queryPayload
                });
                if (resSubjects.statusCode !== OK || !bodySubjects) {
                    let errorString = "Server Error Getting Subject with code" + codePatient;
                    logger.log('error', resSubjects.statusCode);
                    logger.log('error', resSubjects && resSubjects.request && resSubjects.request.body);
                    daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                    return;
                }
                // ricerca soggetto trmite find ( da sviluppare la scelta tra find con e senza paginazione)
                // let [resSubjects, bodySubjects] = yield request.getAsync({
                //     uri: basePath + '/subject?type=' + objInfo.parentSubjectDtId + '&code=' + codePatient,
                //     auth: { bearer: objInfo.bearerToken }
                //     // json: queryPayload
                // });
                // let bodyParsed = JSON.parse(bodySubjects);
                //
                // if (!bodyParsed[0] || !bodyParsed[0].id) {
                //     logger.log('info', "Not found subject with code: " + codePatient);
                //     return BluebirdPromise.rejected("Not found subject with code: " + codePatient);
                // }

                // SE SOGGETTO NON ESISTE ESCO DALLA PROCEDURA
                if (!bodySubjects.data[0]) {
                    let errorString = "Subject with code " + codePatient + " not found";
                    logger.log('info', errorString);
                    daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                    return;
                }
                subject = bodySubjects && bodySubjects.data[0];

                // CONTROLLO CHE IL FILE NON SIA GIÀ STATO INSERITO
                filesAlreadyInserted = subject.metadata && subject.metadata.data_files_inserted ? subject.metadata.data_files_inserted.values : [];
                let alreadyInserted = _.find(filesAlreadyInserted, function (file) {
                    return file === fileName;
                });
                if (alreadyInserted) {
                    let errorString = fileName + " has already been inserted";
                    logger.log('info', fileName + " has already been inserted");
                    daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                    return;
                }
                // CERCO IL TIPO DI DATO TISSUE PER IL PROGETTO SPECIFICO
                //             let [resTissueDt, TissueDt] = yield request.getAsync({
                //                 uri: basePath + '/dataType?project='+objInfo.idProject+"&name=Tissue",
                //                 auth: { bearer: objInfo.bearerToken }
                //             });

                // CERCO IL TESSUTO
                // let prefix = codePatient && codePatient.split('-');
                let biobankCode = codePatient ? codePatient + '-' + tissueCode : tissueCode;

                let querySamplePayload = {
                    "isStream": false,
                    "queryArgs": {
                        "wantsSubject": false,
                        "wantsPersonalInfo": false,
                        "dataType": objInfo.dataTypeId,
                        "model": "Sample",
                        "content": [
                            {
                                "specializedQuery": "Sample",
                                "biobankCode": biobankCode,
                                "codeComparator": "LIKE"
                            },
                            { "specializedQuery": "Sample" }
                        ]
                    }
                };

                let [resTissues, bodyTissues] = yield request.postAsync({
                    uri: basePath + '/query/dataSearch',
                    auth: { bearer: objInfo.bearerToken },
                    json: querySamplePayload
                });
                if (resTissues.statusCode !== OK || !bodyTissues) {
                    let errorString = "Error Getting Tissue";
                    logger.log('error', resTissues.statusCode);
                    logger.log('error', resTissues && resTissues.request && resTissues.request.body);
                    daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                    return;
                }
                // SE IL TESSUTO NON ESISTE LO CREO
                if (!bodyTissues.data[0]) {
                    let typeT;
                    switch (tissueType) {
                        case "BL":
                            typeT = "BLOOD";
                            break;
                        case "FB":
                            typeT = "FYBERBLAST";
                            break;
                        case "UR":
                            typeT = "URINE";
                            break;
                        case "SO":
                            typeT = "SOLID";
                            break;
                        case "SW":
                            typeT = "SWAB";
                            break;
                        default:
                            typeT = "SOLID";

                    }

                    let [resBiobanks, Biobanks] = yield request.getAsync({
                        uri: basePath + '/biobank?project=' + objInfo.idProject,
                        auth: { bearer: objInfo.bearerToken }
                    });

                    let biobanks = JSON.parse(Biobanks);

                    if (!biobanks[0] || !biobanks[0].id) {
                        logger.log('info', "Biobank not found");
                        daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], resBiobanks.body, objInfo.bearerToken);
                        return;
                    }

                    let [resTissueCr, bodyTissueCr] = yield request.postAsync({
                        uri: basePath + '/sample',
                        auth: {
                            bearer: objInfo.bearerToken
                        },
                        json: {
                            type: objInfo.dataTypeId,
                            owner: objInfo.owner,
                            donor: [subject.id],
                            biobank: biobanks[0].id,
                            biobankCode: biobankCode,
                            metadata: {
                                "type": { value: typeT }
                            },
                            tags: null,
                            notes: null
                        }
                    });

                    if (resTissueCr.statusCode !== CREATED) {
                        daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], bodyTissueCr, objInfo.bearerToken);
                        logger.log('error', "Tissue was not correctly created");
                        return;
                    }
                    tissue = bodyTissueCr && bodyTissueCr;
                } else {
                    tissue = bodyTissues && bodyTissues.data[0];
                }

                switch (machine) {
                    case "ILL":
                        mchn = "ILLUMINA";
                        break;
                    case "ION":
                        mchn = "ION TORRENT";
                        break;
                    default:
                        mchn = null;
                }
            }

            // CERCO IL TIPO DI DATO ANALISI PER IL PROGETTO SPECIFICO
            let [resAnalysisDt, AnalysisDt] = yield request.getAsync({
                uri: basePath + '/dataType?project=' + objInfo.idProject + "&name=Variant Call Analysis",
                auth: { bearer: objInfo.bearerToken }
            });

            let analysisDataType = JSON.parse(AnalysisDt);

            if (!analysisDataType[0] || !analysisDataType[0].id) {
                logger.log('info', "Variant Call Analysis dataType not found");
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], resAnalysisDt.body, objInfo.bearerToken);
                return;
            }

            // CREO L'ANALISI
            let [resAnalysisCr, analysis] = yield request.postAsync({
                uri: basePath + '/data',
                auth: {
                    bearer: objInfo.bearerToken
                },
                json: {
                    parentSample: [tissue.id],
                    parentSubject: [subject.id],
                    type: analysisDataType[0].id,
                    owner: objInfo.owner,
                    metadata: {
                        "machine": { value: mchn },
                        "capture": { value: capture }
                    },
                    tags: null,
                    notes: null
                }
            });
            if (resAnalysisCr.statusCode !== CREATED) {
                logger.log('error', "Variant Call Analysis was not correctly created");
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], resAnalysisCr.body, objInfo.bearerToken);
                return;
            }

            //CREO I VCF METADATA
            var count = 0;
            let variants = yield new Promise(function (resolve, reject) {

                vcf.read(file);

                vcf.on('data', function (row) {
                    count += 1;
                    if (row.sampleinfo[0] && subject && subject.id) {
                        let metadataVcf = utils.composeVCFMetadata(row, machine, capture);
                        // if (metadataVcf.length > 1) {
                        //     console.log(metadataVcf);
                        // }
                        metadataVcf.forEach(metadatum => {
                            let json = {
                                owner: parseInt(objInfo.owner),
                                metadata: metadatum,
                                parentData: [analysis.id],
                                parentSubject: [subject.id]
                            };
                            return rowsFilesReady.push(json);

                        });
                    }
                    else {
                        notInserted.push({ index: count, data: row, error: "Row Not Inserted, sampleInfo or Subject not exist" });
                        console.log("Row Not Inserted, sampleInfo or Subject not exist");
                    }
                });

                vcf.on('end', function () {
                    // let now = new Date();
                    // let temp = now - start;
                    // console.log("Execution time read and parse file: %dms", temp);
                    return resolve(rowsFilesReady);
                });

                vcf.on('error', function (err) {
                    return reject(err);
                });
            });

            // CERCO IL TIPO DI DATO VARIANTI PER IL PROGETTO SPECIFICO
            let [resVariantDt, VariantDt] = yield request.getAsync({
                uri: basePath + '/dataType?project=' + objInfo.idProject + "&name=Variant Call",
                auth: { bearer: objInfo.bearerToken }
            });
            let variantDataType = JSON.parse(VariantDt);

            if (!variantDataType[0] || !variantDataType[0].id) {
                logger.log('info', "Variant Call dataType not found");
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], resVariantDt.body, objInfo.bearerToken);
                return;
            }

            // CREO TUTTE LE VARIANTI
            // let last = new Date(), total = 0;
            daemons[daemonIndex].info.totalRows = variants.length;
            daemons[daemonIndex].info.processedRows = created;
            daemons[daemonIndex] = yield DaemonService.UpdateDeamon(daemons[daemonIndex], objInfo.bearerToken);

            yield BluebirdPromise.each(variants, co.wrap(function* (datum, index) {
                datum.type = variantDataType[0].id;
                let chromosome = datum.metadata.chr.value.split('chr');

                // CERCO LE ANNOTAZIONI
                let [freqInfo] = yield knexAnno('frequency_map').select('id_pol', 'af', 'an', 'ac', 'sift', 'polyphen', 'clinvar_meas', 'clinvar_path', 'clinvar_confl', 'clinvar_mut').where({
                    chr: chromosome.length > 1 ? chromosome[1] : chromosome[0],
                    pos: datum.metadata.pos.value,
                    ref: datum.metadata.ref.value,
                    alt: datum.metadata.alt.value
                });

                let [geneInfo] = yield knexAnno('gene_map').select('name').where({
                    chr: chromosome.length > 1 ? chromosome[1] : chromosome[0]
                })
                    .andWhere('start', '<', datum.metadata.pos.value).andWhere('stop', '>', datum.metadata.pos.value);

                datum = utils.composeVCFMetadataAnnotation(datum, freqInfo, geneInfo);

                let [res, body] = yield request.postAsync({
                    uri: basePath + '/data',
                    auth: { bearer: objInfo.bearerToken },
                    json: datum
                });

                if (res.statusCode !== CREATED) {
                    notInserted.push({ index: index, data: datum, error: "Error on Variant Creation" });
                    console.log("Datum Not Inserted: ", datum);
                }
                else {
                    created = created + 1;
                    if (created % 200 === 0) {
                        daemons[daemonIndex].info.processedRows = created;
                        daemons[daemonIndex] = yield DaemonService.UpdateDeamon(daemons[daemonIndex], objInfo.bearerToken);
                    }
                    // let now = new Date();
                    // let temp = now - last;
                    // last = now;
                    // total += temp;
                    // console.log("Execution time last created: %dms", temp, total, created, total/created);
                    return;
                }
            }));

            //AGGIORNO IL SOGGETTO AGGIUNGENDO IL NOME DEL FILE APPENA CARICATO
            let [resSubjectGet, bodySubjectGet] = yield request.getAsync({
                uri: basePath + '/subject/' + subject.id,
                auth: { bearer: objInfo.bearerToken }
            });

            let subjectToUpdate = JSON.parse(bodySubjectGet);

            if (!subjectToUpdate || !subjectToUpdate.id) {
                logger.log('info', "Updating Subject not found");
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], resSubjectGet.body, objInfo.bearerToken);
                return;
            }


            filesAlreadyInserted.push(fileName);
            if (subjectToUpdate.metadata.data_files_inserted) {
                subjectToUpdate.metadata.data_files_inserted.values = filesAlreadyInserted;
            }
            else {
                subjectToUpdate.metadata.data_files_inserted = {
                    "loop": "Data Files",
                    "group": "Inner attributes",
                    "values": filesAlreadyInserted
                };
            }
            let [resSubject, bodySubject] = yield request.putAsync({
                uri: basePath + '/subject/' + subject.id,
                auth: { bearer: objInfo.bearerToken },
                json: subjectToUpdate
            });
            daemons[daemonIndex].info.processedRows = created;
            daemons[daemonIndex].info.notProcessedRows = notInserted;
            daemons[daemonIndex] = yield DaemonService.SuccessDeamon(daemons[daemonIndex], objInfo.bearerToken);
            logger.log("info", fileName + " files stored correctly");
            return;
        }));


        logger.log("info", "All VCF files stored correctly");
        // let end = new Date() - start;
        // console.log("Execution time total: %dms", end);
        return;
    }),

    migrateBioAn: BluebirdPromise.coroutine(function* (folder, ext, processInfo) {

        let objInfo = JSON.parse(processInfo.argv[2]);
        let randomFolder = objInfo.folder;
        folder = randomFolder ? folder + '/' + randomFolder : folder;
        let files = utils.getFilesInFolder(folder, ext);

        if (files.length === 0) {
            let errorString = "Invalid or no files loaded";
            let daemon = yield DaemonService.InitializeDeamon(" ", {}, objInfo.executor, processInfo, objInfo.bearerToken);
            daemon = yield DaemonService.ErrorDeamon(daemon, errorString, objInfo.bearerToken);
            logger.log('info', "Invalid or no files loaded");
            return BluebirdPromise.rejected(errorString);
        } else if (files.length > 1) {
            let errorString = "Load at most one file";
            let daemon = yield DaemonService.InitializeDeamon(" ", {}, objInfo.executor, processInfo, objInfo.bearerToken);
            daemon = yield DaemonService.ErrorDeamon(daemon, errorString, objInfo.bearerToken);
            logger.log('info', "Load at most one file");
            return BluebirdPromise.rejected(errorString);
        }

        let daemons = [], rowsTobeProcessed = [];

        const workbook = xlsx.readFile(files[0]);
        const worksheet1 = workbook.Sheets[workbook.SheetNames[0]];
        const range1 = xlsx.utils.decode_range(worksheet1['!ref']);

        //Create the json file from xlsx file
        const data = xlsx.utils.sheet_to_json(worksheet1);

        yield BluebirdPromise.each(data, co.wrap(function* (datum, index) {

            if (!datum['RINB']) {
                let errorString = 'No RINB Code for row ' + index;
                let daemon = yield DaemonService.InitializeDeamon(" ", {}, objInfo.executor, processInfo, objInfo.bearerToken);
                daemon = yield DaemonService.ErrorDeamon(daemon, errorString, objInfo.bearerToken);
                logger.log('info', errorString);
                return BluebirdPromise.rejected(errorString);
            }
            let dmn = yield DaemonService.InitializeDeamon(datum['RINB'], {}, objInfo.executor, processInfo, objInfo.bearerToken);
            if (dmn && dmn.status === 'initializing') {
                rowsTobeProcessed.push(datum['RINB']);
                daemons.push(dmn);
            }
        }));

        yield BluebirdPromise.each(data, co.wrap(function* (datum, daemonIndex) {
            let notInserted = [], created = 0;
            let fileName = datum['RINB'];
            logger.log('info', "Migrator.migrateBioAn - RINB: ", datum['RINB']);


            let metadata = utils.composeBioAnMetadata(datum);

            daemons[daemonIndex].info.totalRows = 1;
            daemons[daemonIndex].info.processedRows = 0;
            daemons[daemonIndex] = yield DaemonService.UpdateDeamon(daemons[daemonIndex], objInfo.bearerToken);

            logger.log('info', "Migrator.migrateBioAn - here we are");

            let queryPayload = {
                "queryArgs": {
                    "wantsSubject": true,
                    "dataType": 1,
                    "model": "Subject",
                    "content": [{
                        "dataType": 16,
                        "model": "Data",
                        "content": [{
                            "fieldName": "italian_nb_registry_id",
                            "fieldType": "integer",
                            "comparator": "=",
                            "fieldValue": parseInt(datum['RINB'])
                        }]
                    }]
                }
            };

            let [res, body] = yield request.postAsync({
                uri: basePath + '/query/dataSearch',
                auth: { bearer: objInfo.bearerToken },
                json: queryPayload
            });
            if (res.statusCode !== OK || !body) {
                logger.log('error', res.statusCode);
                logger.log('error', res && res.request && res.request.body);
                logger.log('info', 'Skipping Row: ' + datum['RINB']);
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], res.body, objInfo.bearerToken);

                return;
            }
            if (!body.data[0]) {
                let errorString = "Migrator.migrateBioAn: no patient found with RINB " + datum['RINB'];
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                return;
            }

            let idSubj = body.data[0].id, idSample;

            if (!datum['PLASMA']) {

                let [resTissue, bodySample] = yield request.postAsync({
                    uri: basePath + '/sample',
                    auth: {
                        bearer: objInfo.bearerToken
                    },
                    json: {
                        type: 3,
                        owner: 28,
                        donor: [idSubj],
                        biobank: 1, //BIT
                        metadata: metadata.sample,
                        tags: null,
                        notes: datum['Sample_Notes'] ? datum['Sample_Notes'] : null
                    }
                });


                if (resTissue.statusCode !== CREATED || !bodySample) {
                    logger.log('error', "Error Creating sample");
                    daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], resTissue.body, objInfo.bearerToken);
                    return;
                }

                if (!bodySample) {
                    let errorString = "Migrator.migrateCGH: no DNA found";
                    daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                    return;
                }

                idSample = bodySample.id;

            } else {
                let queryPayloadPlasma = {
                    "queryArgs": {
                        "wantsSubject": false,
                        "dataType": 3,
                        "model": "Sample",
                        "content": [{
                            "specializedQuery": "Sample",
                            "biobankCode": datum['PLASMA'],
                            "biobankCodeComparator": "="
                        }, {
                            "specializedQuery": "Sample"
                        }]
                    }
                };

                let [resPlasma, bodyPlasma] = yield request.postAsync({
                    uri: basePath + '/query/dataSearch',
                    auth: { bearer: objInfo.bearerToken },
                    json: queryPayload
                });
                if (resPlasma.statusCode !== OK || !bodyPlasma) {
                    logger.log('error', resPlasma.statusCode);
                    logger.log('error', resPlasma && resPlasma.request && resPlasma.request.body);
                    logger.log('info', 'Skipping Row: ' + datum['RINB']);
                    daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], res.body, objInfo.bearerToken);

                    return;
                }
                if (!bodyPlasma.data[0]) {
                    let errorString = "Migrator.migrateBioAn: no Plasma found for RINB " + datum['RINB'];
                    daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                    return;
                }

                idSample = bodyPlasma.data[0].id;
            }

            let [resBioAN, bodyBioAN] = yield request.postAsync({
                uri: basePath + '/data',
                auth: {
                    bearer: objInfo.bearerToken
                },
                json: {
                    type: 204, //Biochemistry Analysis
                    owner: objInfo.owner,
                    metadata: metadata.bioAn,
                    parentSubject: [idSubj],
                    parentSample: [idSample],
                    date: datum['Analysis Date'] ? moment(datum['Analysis Date'], "DD-MM-YYYY").format("YYYY-MM-DD") : null,
                    notes: datum['BioAn_Notes'] ? datum['BioAn_Notes'] : null,
                    tags: null
                }
            });

            if (resBioAN.statusCode !== CREATED) {
                notInserted.push({ index: 1, data: datum, error: "Error on biochemistry analysis creation" });
            }
            else {
                created = 1;
                daemons[daemonIndex].info.processedRows = 1;
                daemons[daemonIndex] = yield DaemonService.UpdateDeamon(daemons[daemonIndex], objInfo.bearerToken);
            }

            logger.log('info', "Migrator.migrateBioAN -  done for RINB:" + datum['RINB']);
            daemons[daemonIndex].info.processedRows = created;
            daemons[daemonIndex].info.notProcessedRows = notInserted;
            daemons[daemonIndex] = yield DaemonService.SuccessDeamon(daemons[daemonIndex], objInfo.bearerToken);
            return;
        }));


        logger.log("info", "All Biochemistry Analysis data were stored correctly");
        return;
    }),

    migrateNKCells: BluebirdPromise.coroutine(function* (folder, ext, processInfo) {

        let objInfo = JSON.parse(processInfo.argv[2]);
        let randomFolder = objInfo.folder;
        folder = randomFolder ? folder + '/' + randomFolder : folder;
        let files = utils.getFilesInFolder(folder, ext);

        if (files.length === 0) {
            let errorString = "Invalid or no files loaded";
            let daemon = yield DaemonService.InitializeDeamon(" ", {}, objInfo.executor, processInfo, objInfo.bearerToken);
            daemon = yield DaemonService.ErrorDeamon(daemon, errorString, objInfo.bearerToken);
            logger.log('info', "Invalid or no files loaded");
            return BluebirdPromise.rejected(errorString);
        } else if (files.length > 1) {
            let errorString = "Load at most one file";
            let daemon = yield DaemonService.InitializeDeamon(" ", {}, objInfo.executor, processInfo, objInfo.bearerToken);
            daemon = yield DaemonService.ErrorDeamon(daemon, errorString, objInfo.bearerToken);
            logger.log('info', "Load at most one file");
            return BluebirdPromise.rejected(errorString);
        }

        let daemons = [], rowsTobeProcessed = [];

        const workbook = xlsx.readFile(files[0]);
        const worksheet1 = workbook.Sheets[workbook.SheetNames[0]];
        const range1 = xlsx.utils.decode_range(worksheet1['!ref']);

        //Create the json file from xlsx file
        const data = xlsx.utils.sheet_to_json(worksheet1);

        yield BluebirdPromise.each(data, co.wrap(function* (datum, index) {

            if (!datum['FLUIDO']) {
                let errorString = 'No Biobank Code for row ' + index;
                let daemon = yield DaemonService.InitializeDeamon(" ", {}, objInfo.executor, processInfo, objInfo.bearerToken);
                daemon = yield DaemonService.ErrorDeamon(daemon, errorString, objInfo.bearerToken);
                logger.log('info', errorString);
                return BluebirdPromise.rejected(errorString);
            }
            let dmn = yield DaemonService.InitializeDeamon(datum['FLUIDO'], {}, objInfo.executor, processInfo, objInfo.bearerToken);
            if (dmn && (dmn.status === 'initializing' || dmn.status === 'error')) {
                rowsTobeProcessed.push(datum['FLUIDO']);
                daemons.push(dmn);
            }
        }));

        yield BluebirdPromise.each(data, co.wrap(function* (datum, daemonIndex) {
            let notInserted = [], created = 0;
            let fileName = datum['FLUIDO'];
            logger.log('info', "Migrator.migrateNKCells - FLUIDO: ", datum['FLUIDO']);


            let nkcellMetadata = utils.composeNKCellsMetadata(datum);

            daemons[daemonIndex].info.totalRows = 1;
            daemons[daemonIndex].info.processedRows = 0;
            daemons[daemonIndex] = yield DaemonService.UpdateDeamon(daemons[daemonIndex], objInfo.bearerToken);

            logger.log('info', "Migrator.migrateNKCells - here we are");

            let [resTissue, bodySample] = yield request.getAsync({
                uri: basePath + '/sample?biobankCode=' + datum['FLUIDO'],
                auth: {
                    bearer: objInfo.bearerToken
                }
            });


            if (resTissue.statusCode !== OK || !JSON.parse(bodySample)) {
                logger.log('error', "Error finding sample");
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], resTissue.body, objInfo.bearerToken);
                return;
            }

            if (!bodySample) {
                let errorString = "Migrator.migrateNKCells: no FLUID found";
                daemons[daemonIndex] = yield DaemonService.ErrorDeamon(daemons[daemonIndex], errorString, objInfo.bearerToken);
                return;
            }
            const fluid = JSON.parse(bodySample)[0];
            const idSample = fluid.id;
            const idSubj = fluid.donor;



            let [resNKCells, bodyNKCells] = yield request.postAsync({
                uri: basePath + '/data',
                auth: {
                    bearer: objInfo.bearerToken
                },
                json: {
                    type: 216, //NK Cells Analysis data type
                    owner: objInfo.owner,
                    metadata: nkcellMetadata,
                    parentSubject: [idSubj],
                    parentSample: [idSample],
                    date: datum['DATA ANALISI'] ? moment(datum['DATA ANALISI'], "DD-MM-YYYY").format("YYYY-MM-DD") : null,
                    notes: datum['NOTE'] ? datum['NOTE'] : null,
                    tags: null
                }
            });

            if (resNKCells.statusCode !== CREATED) {
                notInserted.push({ index: 1, data: datum, error: "Error on nkcells analysis creation" });
            }
            else {
                created = 1;
                daemons[daemonIndex].info.processedRows = 1;
                daemons[daemonIndex] = yield DaemonService.UpdateDeamon(daemons[daemonIndex], objInfo.bearerToken);
            }

            logger.log('info', "Migrator.migrateNKCells -  done for Biobank Code:" + datum['FLUIDO']);
            daemons[daemonIndex].info.processedRows = created;
            daemons[daemonIndex].info.notProcessedRows = notInserted;
            daemons[daemonIndex] = yield DaemonService.SuccessDeamon(daemons[daemonIndex], objInfo.bearerToken);
            return;
        }));


        logger.log("info", "All NK Cells Phenotype and Function Analysis data were stored correctly");
        return;
    }),

    /**
     * @method
     * @name importCNBInfo
     * @param{Integer - Array} groupsId
     * @param{Integer - Array} dataTypesId
     * @description coroutine for get DataTypes' privileges
     */
    importCNBInfo: BluebirdPromise.coroutine(function* (folder, ext) {
        let files = utils.getFilesInFolder(folder, ext);

        if (_.isEmpty(files)) {
            logger.log('info', "Invalid or no files loaded");
            return BluebirdPromise.rejected("No Valid files loaded");
        }


        let workbook = xlsx.readFile(files[0]);
        let worksheet = workbook.Sheets[workbook.SheetNames[0]];
        let range = xlsx.utils.decode_range(worksheet['!ref']);

        //Create the json file from xlsx file
        let patients = xlsx.utils.sheet_to_json(worksheet);

        // if (!patients[0]['Registry ID']) {
        //     return BluebirdPromise.rejected("No Valid files loaded");
        // }

        let updated = 0, created = 0;
        let name, surname, birthDate, idSubject, metadataCNBInfo, subjects, CNBInfos;

        let queryPayload = {
            "isStream": false,
            "queryArgs": {
                "wantsSubject": true,
                "wantsPersonalInfo": true,
                "dataType": 1,
                "model": "Subject",
                "content": [
                    {
                        "personalDetails": true
                    }, {
                        "specializedQuery": "Subject"
                    }, {
                        "specializedQuery": "Subject"
                    }
                ]
            }
        };
        let [resSubjects, bodySubjects] = yield request.postAsync({
            uri: basePath + '/query/dataSearch',
            auth: {
                bearer: connections.bearerToken
            },
            json: queryPayload
        });

        if (resSubjects.statusCode !== OK || !bodySubjects) {
            logger.log('error', resSubjects.statusCode);
            logger.log('error', resSubjects && resSubjects.request && resSubjects.request.bodySubjects);
            return BluebirdPromise.rejected("Error loading Data");
        }

        subjects = bodySubjects && bodySubjects.data;

        let [resClInfo, bodyClInfo] = yield request.postAsync({
            uri: basePath + '/query/dataSearch',
            auth: { bearer: connections.bearerToken },
            json: {
                "queryArgs": {
                    "wantsPersonalInfo": true,
                    "wantsSubject": true,
                    "dataType": NB_CLINICAL_SITUATION_POSTGRES_ID,
                    "model": "Data"
                }
            }
        });

        if (resClInfo.statusCode !== OK || !bodyClInfo) {
            logger.log('error', resClInfo.statusCode);
            logger.log('error', resClInfo && resClInfo.request && resClInfo.request.body);
            return BluebirdPromise.rejected("Error loading CLinical Info");
        }

        CNBInfos = bodyClInfo && bodyClInfo.data;

        let cbInfosRemains = CNBInfos;
        let results = yield BluebirdPromise.each(patients, co.wrap(function* (patient) {
            let lc, res;
            let CNBInfo = patient['Registry ID'] ? _.find(CNBInfos, function (obj) {
                return obj.metadata['italian_nb_registry_id'].value == patient['Registry ID'];
            }) : undefined;

            cbInfosRemains = _.filter(cbInfosRemains, function (obj) {
                return obj.metadata['italian_nb_registry_id'].value != patient['Registry ID'];
            });

            if (CNBInfo) {
                let metadataCNBInfo = utils.composeCNBInfoMetadata(patient);

                let idCNBInfo = CNBInfo.id;
                updated = updated + 1;

                logger.log('info', "Migrator.importCNBInfo - patient: " + patient.NOME + " " + patient.COGNOME + " CNBInfo to be updated. " + CNBInfo.id + " " + updated);

                let [resUpdate, bodyUpdate] = yield request.putAsync({
                    uri: basePath + '/data/' + idCNBInfo,
                    auth: {
                        bearer: connections.bearerToken
                    },
                    json: {
                        id: idCNBInfo,
                        date: moment().format("YYYY-MM-DD"),
                        owner: 28,
                        type: NB_CLINICAL_SITUATION_POSTGRES_ID,
                        metadata: metadataCNBInfo
                    }
                });
                return bodyUpdate;
                // return {};
            }
            else {
                if (patient['COGNOME']) {

                    let birthdate = patient['DATA_NASCITA'] && moment(patient['DATA_NASCITA']).format("DD/MM/YYYY");

                    let subject = _.find(subjects, function (obj) {
                        return (moment(obj.birth_date).format("DD/MM/YYYY") == birthdate && obj['surname'] == patient['COGNOME'].toUpperCase().trim() && obj['given_name'] == patient['NOME'].toUpperCase().trim()) ||
                            (obj['surname'] == patient['COGNOME'].toUpperCase().trim()
                                && moment(obj.birth_date).format("DD/MM/YYYY") == birthdate) ||
                            (obj['given_name'] == patient['NOME'].toUpperCase().trim()
                                && moment(obj.birth_date).format("DD/MM/YYYY") == birthdate ||
                                (obj['given_name'] == patient['NOME'].toUpperCase().trim()
                                    && obj['surname'] == patient['COGNOME'].toUpperCase().trim()));
                    });

                    if (subject && patient['Registry ID']) {

                        let metadataCNBInfo = utils.composeCNBInfoMetadata(patient);

                        let idSubject = subject.id;
                        created = created + 1;
                        logger.log('info', "Migrator.importCNBInfo - patient " + idSubject + " : " + patient.NOME + " " + patient.COGNOME + " CNBInfo to be created. " + created);

                        let [resCreate, bodyCreate] = yield request.postAsync({
                            uri: basePath + '/data',
                            auth: {
                                bearer: connections.bearerToken
                            },
                            json: {
                                date: moment().format("YYYY-MM-DD"),
                                type: NB_CLINICAL_SITUATION_POSTGRES_ID,
                                owner: 28,
                                parentSubject: [idSubject],
                                metadata: metadataCNBInfo
                            }
                        });
                        return bodyCreate;
                    }
                    else {
                        logger.log('info', "Migrator.importCNBInfo - patient: " + patient.NOME + " " + patient.COGNOME + " with code: " + patient['Registry ID'] + " is not present into DB. ");
                    }
                }
            }
        }));
        logger.log('info', "Migrator.importCNBInfo - " + created + " CNBInfo created and " + updated + " updated correctly.");
        return { created: created, updated: updated, notUpdated: cbInfosRemains.length };
    }),



    GetInsertSampleNGSforPT: BluebirdPromise.coroutine(function* (patient, daemons) {

         let queryPayload = {
                "queryArgs": {
                    "dataType": 211,
                    "model": "Sample",
                    "content": [{
                        "fieldName": "laboratory_id",
                        "fieldType": "text",
                        "comparator": "=",
                        "fieldValue": patient['SAMPLE ID (LAB)']
                    }]
                }
            };
    
    
            let [resSubject, bodySubject] = yield request.postAsync({
                uri: basePath + '/query/dataSearch',
                auth: {
                    bearer: connections.bearerToken
                },
                json: queryPayload
            });
    
            if (resSubject.statusCode !== OK || !bodySubject) {
                logger.log('error', resSubject.statusCode);
                logger.log('error', resSubject && resSubject.request && resSubject.request.bodySubject);
                daemons[patient['SAMPLE ID (LAB)']] = yield DaemonService.ErrorDeamon(daemons[patient['SAMPLE ID (LAB)']], resSubject.body, connections.bearerToken);
                return;
            }
    
            if (bodySubject && bodySubject.data[0]) {
                
                patient.idSample = bodySubject.data[0].id;
            } else { 

                let [resCreaSubj, bodyCreaSubj] = yield request.postAsync({
                    uri: basePath + '/sample',
                    auth: {
                        bearer: connections.bearerToken
                    },
                    json: {
                        type: 211, // Tissue
                        owner: 45,
                        biobank: 8, 
                        metadata: {
                            "type": {
                              "group": "Details",
                              "value": "BLOOD"
                            },
                            "_260_280": {
                              "group": "Details",
                              "value": patient['260/280']
                            },
                            "total_dna": {
                              "unit": "ng",
                              "group": "Details",
                              "value": patient['Total DNA']
                            },
                            "dna_concentration": {
                              "unit": "ng/ul",
                              "group": "Details",
                              "value": patient['DNA Concentration']
                            },
                            "volume": {
                              "unit": "ul",
                              "group": "Details",
                              "value": patient['Volume']
                            },
                            "iit_shipment_date": {
                              "group": "Details",
                              "value": patient['IIT Shipment Date'] ? moment.tz(patient['IIT Shipment Date'], "Europe/Rome").format("YYYY-MM-DD") : null,
                            },
                            "laboratory_id": {
                                "group": "External Codes",
                                "value": patient['SAMPLE ID (LAB)']
                            },
                            "iit_id": {
                                "group": "External Codes",
                                "value": patient['Sample ID (IIT)']
                            }
                          },
                        // parentSample: [idSample],
                        donor: [patient.id]
                    }
                });
    
                if (resCreaSubj.statusCode !== CREATED) {
                    let errorString = "SAMPLE was not correctly created. " + bodyCreaSubj.error.message.details[0].message;
                    logger.log('info', errorString);
                    daemons[patient['SAMPLE ID (LAB)']] = yield DaemonService.ErrorDeamon(daemons[patient['SAMPLE ID (LAB)']], errorString, connections.bearerToken);
                    return;
                }

                patient.idSample = bodyCreaSubj.id;
            }

            return {
                "patient": patient,
                "daemons": daemons
            };
        }),

    GetInsertAnalysisNGSforPT: BluebirdPromise.coroutine(function* (patient, daemons) {

            let targetAnalysis = patient['Seq Type'] ? patient['Seq Type'].toUpperCase() === 'WGS' ? 'WHOLE GENOME' : patient['Seq Type'].toUpperCase() === 'WES' ? "EXOME" : patient['Seq Type'].toUpperCase() === 'PANEL' ? "PANEL" : null : null;
            if (targetAnalysis === null) {
                return {
                    "patient": patient,
                    "daemons": daemons
                };
            }
            let [resAnalysis, bodyA] = yield request.getAsync({
                uri: basePath + '/data?parentSample=' + patient.idSample + '&parentSubject=' + patient.id,
                auth: {
                    bearer: connections.bearerToken
                }
            });
            if (resAnalysis.statusCode !== OK || !bodyA) {
                let daemonKey = patient['SAMPLE ID (LAB)'];
                daemons[daemonKey] = yield DaemonService.ErrorDeamon(daemons[daemonKey], resAnalysis.body, connections.bearerToken);
                return;
            }
            let BodyAnalysis = JSON.parse(bodyA);

            if (BodyAnalysis && _.isArray(BodyAnalysis) && BodyAnalysis.length > 0) {
                let foundAnalysis =  BodyAnalysis[0];
                if(foundAnalysis.metadata.target.value === targetAnalysis) {
                    patient.idAnalysis = foundAnalysis.id;
                } else {
                    let [resCreaAn, bodyCreaAn] = yield request.postAsync({
                        uri: basePath + '/data',
                        auth: {
                            bearer: connections.bearerToken
                        },
                        json: {
                            type: 212, // Analysis
                            owner: 45,
                            metadata: {
                                "target": {
                                  "group": "Details",
                                  "value": targetAnalysis
                                },
                             //    "platform": {
                             //      "group": "Details",
                             //      "value": patient['Platform']
                             //    },
                             //    "results__date": {
                             //      "group": "Details",  //impostati da CORE DI BIOINFO
                             //      "value": "2020-10-01"
                             //    },
                             //    "target_details": {
                             //      "group": "Details",
                             //      "value": patient['KIT']
                             //    }
                             },
                            parentSample: [patient.idSample],
                            parentSubject: [patient.id]
                        }
                    });
        
                    if (resCreaAn.statusCode !== CREATED) {
                        let errorString = "SUBJECT was not correctly created. " + bodyCreaAn.error.message.details[0].message;
                        logger.log('info', errorString);
                        daemons[patient['SAMPLE ID (LAB)']] = yield DaemonService.ErrorDeamon(daemons[patient['SAMPLE ID (LAB)']], resCreaAn.body, connections.bearerToken);
                        return;
                    }
    
                    patient.idAnalysis = bodyCreaAn.id;
                }
            }
               else { 
   
                   let [resCreaAn, bodyCreaAn] = yield request.postAsync({
                       uri: basePath + '/data',
                       auth: {
                           bearer: connections.bearerToken
                       },
                       json: {
                           type: 212, // Analysis
                           owner: 45,
                           metadata: {
                               "target": {
                                 "group": "Details",
                                 "value": targetAnalysis
                               },
                            //    "platform": {
                            //      "group": "Details",
                            //      "value": patient['Platform']
                            //    },
                            //    "results__date": {
                            //      "group": "Details",  //impostati da CORE DI BIOINFO
                            //      "value": "2020-10-01"
                            //    },
                            //    "target_details": {
                            //      "group": "Details",
                            //      "value": patient['KIT']
                            //    }
                            },
                           parentSample: [patient.idSample],
                           parentSubject: [patient.id]
                       }
                   });
       
                   if (resCreaAn.statusCode !== CREATED) {
                       let errorString = "SUBJECT was not correctly created. " + bodyCreaAn.error.message.details[0].message;
                       logger.log('info', errorString);
                       daemons[patient['SAMPLE ID (LAB)']] = yield DaemonService.ErrorDeamon(daemons[patient['SAMPLE ID (LAB)']], resCreaAn.body, connections.bearerToken);
                       return;
                   }
   
                   patient.idAnalysis = bodyCreaAn.id;
               }
   
               return {
                   "patient": patient,
                   "daemons": daemons
               };
           }),
   
    GetInsertPatientNGSforPT: BluebirdPromise.coroutine(function* (patient, daemons) {
       
        let queryPayload = {
            "queryArgs": {
                "wantsSubject": true,
                "dataType": 210,
                "model": "Subject",
                "content": [
                //     {
                //     "fieldName": "family_id",
                //     "fieldType": "text",
                //     "comparator": "=",
                //     "fieldValue": patient['Family ID']
                // },
                 {
                    "fieldName": "status",
                    "fieldType": "text",
                    "comparator": "=",
                    "fieldValue": patient['Relationship']
                }, {
                    "dataType": 211,
                "model": "Sample",
                "content": [{
                    "fieldName": "laboratory_id",
                    "fieldType": "text",
                    "comparator": "=",
                    "fieldValue": patient['SAMPLE ID (LAB)']
                    }]
                }]
            }
        };    

        let [resSubject, bodySubject] = yield request.postAsync({
            uri: basePath + '/query/dataSearch',
            auth: {
                bearer: connections.bearerToken
            },
            json: queryPayload
        });

        if (resSubject.statusCode !== OK || !bodySubject) {
            logger.log('error', resSubject.statusCode);
            logger.log('error', resSubject && resSubject.request && resSubject.request.bodySubject);
            daemons[patient['SAMPLE ID (LAB)']] = yield DaemonService.ErrorDeamon(daemons[patient['SAMPLE ID (LAB)']], resSubject.body, connections.bearerToken);
            return;
        }

        if (bodySubject && bodySubject.data[0]) {
            
            patient.id = bodySubject.data[0].id;
        } else {
            let [resCreaSubj, bodyCreaSubj] = yield request.postAsync({
                uri: basePath + '/subject',
                auth: {
                    bearer: connections.bearerToken
                },
                json: {
                    type: 210, // CGH Raw type
                    owner: 45,
                    sex: patient['Sex'].toUpperCase(),
                    metadata: {
                        "status": {
                            "group": "Details",
                            "value": patient['Relationship']
                        },
                        "affected": {
                            "group": "Details",
                            "value": patient['Status'].toUpperCase() === 'AFFECTED' ? true : false
                        },
                        "family_id": {
                            "group": "Details",
                            "value": patient['Family ID']
                        },
                        "phenotips_id": {
                            "group": "External Codes",
                            "value": patient['PHENOTIPS ID']
                        },
                        "unit": {
                            "group": "Provenance Info",
                            "value": patient['Unit']
                        }
                      },
                    // parentSample: [idSample],
                    parentSubject: patient.idParentSubject && patient.idParentSubject.length > 0 ? patient.idParentSubject : null
                }
            });

            if (resCreaSubj.statusCode !== CREATED) {
                let errorString = "SUBJECT was not correctly created. " + bodyCreaSubj.error.message.details[0].message;
                logger.log('info', errorString);
                daemons[patient['SAMPLE ID (LAB)']] = yield DaemonService.ErrorDeamon(daemons[patient['SAMPLE ID (LAB)']], resCreaSubj.body, connections.bearerToken);
                return;
            }

            patient.id = bodyCreaSubj.id;
        }

        return {
            "patient": patient,
            "daemons": daemons
        };

    }),


     /**
     * @method
     * @name migrateNGSPATIENTS
     * @description coroutine for import NGS Patients
     */
    migrateNGSPATIENTS: BluebirdPromise.coroutine(function* (defaultPath, ext, process) {
        var that = this;
        let objInfo = JSON.parse(process.argv[2]);
        let files = utils.getFilesInFolder(defaultPath + '/' + objInfo.folder, ext);

        if (_.isEmpty(files)) {
            logger.log('info', "Invalid or no files loaded");
            return BluebirdPromise.rejected("No Valid files loaded");
        }


        let workbook = xlsx.readFile(files[0]);

        let worksheet;
        if(!workbook.Sheets.hasOwnProperty('Data Import')) {
            worksheet = workbook.Sheets[workbook.SheetNames[0]]
        } else if (workbook.Sheets.hasOwnProperty('Data Import')) {    
            worksheet = workbook.Sheets['Data Import'];
        }

        //Create the json file from xlsx file
        let patients = xlsx.utils.sheet_to_json(worksheet);

        if(!patients || (patients && !_.isArray(patients)) || (patients && _.isArray(patients) && patients.length == 0)) {
            let errorString = 'No rows found in sheet: ' + workbook.SheetNames[0] + '. Please check your xlsm file. Be sure data to load are into the first sheet or rename sheet with \'Data Import\'.';
            let daemon = yield DaemonService.InitializeDeamon(files[0], {}, objInfo.executor, process, connections.bearerToken);
            daemon = yield DaemonService.ErrorDeamon(daemon, errorString, connections.bearerToken);
            logger.log('info', errorString);
            return BluebirdPromise.rejected(errorString);
        }

        patients = _.map(patients, (pat) => { 
            if(pat['Family ID']){
                pat['Family ID'] = pat['Family ID'].toString().trim();
            }
            if(pat['SAMPLE ID (LAB)']) {
                pat['SAMPLE ID (LAB)'] = pat['SAMPLE ID (LAB)'].toString().trim();
            }
            return pat;
        });

        let updated = 0, created = 0, daemons = {};

        const families = _.uniq(_.map(patients, 'Family ID'));

    yield BluebirdPromise.each(patients, co.wrap(function* (patient, index) {

        if (!patient['Family ID']) {
            let errorString = 'No Family ID, row: ' + index;
            let daemon = yield DaemonService.InitializeDeamon(files[0], {}, objInfo.executor, process, connections.bearerToken);
            daemon = yield DaemonService.ErrorDeamon(daemon, errorString, connections.bearerToken);
            logger.log('info', errorString);
            return BluebirdPromise.rejected(errorString);
        } else if (!patient['SAMPLE ID (LAB)']) {
            let errorString = 'No SAMPLE ID (LAB), row: ' + index;
            let daemon = yield DaemonService.InitializeDeamon(files[0], {}, objInfo.executor, process, connections.bearerToken);
            daemon = yield DaemonService.ErrorDeamon(daemon, errorString, connections.bearerToken);
            logger.log('info', errorString);
            return BluebirdPromise.rejected(errorString);
        }
        else {
                let dmn = yield DaemonService.InitializeDeamon(patient['SAMPLE ID (LAB)'], {}, objInfo.executor, process, connections.bearerToken);
                if (dmn && (dmn.status === 'initializing' || dmn.status === 'error')) {
                    // rowsTobeProcessed.push(datum['FLUIDO']);
                    daemons[patient['SAMPLE ID (LAB)']] = dmn;
            }
        }
    }));
    
        yield BluebirdPromise.each(families, co.wrap(function* (familyID) {

            let family = _.filter(patients, {'Family ID': familyID})
            let probands = _.filter(family, {'Relationship': 'PROBAND'});

            let probandsID = [];

            yield BluebirdPromise.each(probands, co.wrap(function* (proband) {

                daemons[proband['SAMPLE ID (LAB)']].info.totalRows = 1;
                daemons[proband['SAMPLE ID (LAB)']].info.processedRows = 0;
                daemons[proband['SAMPLE ID (LAB)']] = yield DaemonService.UpdateDeamon(daemons[proband['SAMPLE ID (LAB)']], connections.bearerToken);

                //TODO CERCO E CREO IL PROBANDO

                let resPat = yield that.GetInsertPatientNGSforPT(proband, daemons);
                proband = resPat.patient;
                daemons = resPat.daemons;

                probandsID.push(proband.id);

                const resSample = yield that.GetInsertSampleNGSforPT(proband, daemons);
                if(!resSample.patient) return;
                proband = resSample.patient;
                daemons = resSample.daemons;

                if (!proband['Seq Type'] != undefined && !proband['Seq Type'] != null) { //&& !relative['Platform'] && !relative['KIT']
                    const resAnalysis = yield that.GetInsertAnalysisNGSforPT(proband, daemons);
                    if(!resAnalysis.patient) return;
                    proband = resAnalysis.patient;
                    daemons = resAnalysis.daemons;
                }

                daemons[proband['SAMPLE ID (LAB)']].info.processedRows = 1;
                daemons[proband['SAMPLE ID (LAB)']].info.notProcessedRows = [];
                daemons[proband['SAMPLE ID (LAB)']] = yield DaemonService.SuccessDeamon(daemons[proband['SAMPLE ID (LAB)']], connections.bearerToken);

            }));
            //foreach family

            yield BluebirdPromise.each(family, co.wrap(function* (relative) {
                if(relative['Relationship'] === 'PROBAND') {
                    return;
                }
                if (probandsID.length === 0)
                {
                    let errorString = "No proband found for family " + familyID + ". Relative " + relative['SAMPLE ID (LAB)'] + " skipped";
                    logger.log('info', errorString);
                    daemons[relative['SAMPLE ID (LAB)']] = yield DaemonService.ErrorDeamon(daemons[relative['SAMPLE ID (LAB)']], errorString, connections.bearerToken);
                    return;
                }

                relative.idParentSubject = probandsID;

                daemons[relative['SAMPLE ID (LAB)']].info.totalRows = 1;
                daemons[relative['SAMPLE ID (LAB)']].info.processedRows = 0;
                daemons[relative['SAMPLE ID (LAB)']] = yield DaemonService.UpdateDeamon(daemons[relative['SAMPLE ID (LAB)']], connections.bearerToken);

                let resPat = yield that.GetInsertPatientNGSforPT(relative, daemons);
                relative = resPat.patient;
                daemons = resPat.daemons;

                const resSample = yield that.GetInsertSampleNGSforPT(relative, daemons);
                if(!resSample.patient) return;
                relative = resSample.patient;
                daemons = resSample.daemons;


                if (!relative['Seq Type'] != undefined && !relative['Seq Type'] != null) { //&& !relative['Platform'] && !relative['KIT']
                    const resAnalysis = yield that.GetInsertAnalysisNGSforPT(relative, daemons);
                    if(!resAnalysis.patient) return;
                    relative = resAnalysis.patient;
                    daemons = resAnalysis.daemons;
                }

                daemons[relative['SAMPLE ID (LAB)']].info.processedRows = 1;
                daemons[relative['SAMPLE ID (LAB)']].info.notProcessedRows = [];
                daemons[relative['SAMPLE ID (LAB)']] = yield DaemonService.SuccessDeamon(daemons[relative['SAMPLE ID (LAB)']], connections.bearerToken);
                
            }));

            logger.log('info', "Migrator.createNGSPatients - Family: " + familyID + " correctly imported.");
 
        }));

        logger.log('info', "Migrator.createNGSPatients - all families correctly imported.");

        return { created: 0, updated: 0, notUpdated: 0 };
    }),


    GetPatientAndSampleNGSforAN: BluebirdPromise.coroutine(function* (analysis, daemon) {
        let biobankCode, donor, subjectCode, idSample;
        let queryPayload = {
               "queryArgs": {
                   "dataType": 211,
                   "model": "Sample",
                   "content": []
               }
           };

           if (analysis["sample"]) {
               queryPayload.queryArgs.content.push({
                   "specializedQuery": "Sample",
                   "biobankCode": analysis['sample'],
                   "biobankCodeComparator": "="
               });
            } 
            else {
                let errorString = 'No Sample ID provided. Please fill "sample" or "XTENS ID" field'
                logger.log('error', errorString);
                daemon = yield DaemonService.ErrorDeamon(daemon, errorString, connections.bearerToken);
                return;
            }
   
   
           let [resSample, resBodySample] = yield request.getAsync({
               uri: basePath + '/sample?biobankCode=' + analysis['sample'],
               auth: {
                   bearer: connections.bearerToken
               },
            //    json: queryPayload
           });

           if (resSample.statusCode !== OK || !resBodySample) {
            let daemonKey = analysis['sample'];
            daemon = yield DaemonService.ErrorDeamon(daemon, resSample.body, connections.bearerToken);
            return;
        }

        let bodySample = JSON.parse(resBodySample);

        if (bodySample && _.isArray(bodySample) && bodySample.length > 0) {
            donor = bodySample[0].donor;
            biobankCode = bodySample[0].biobankCode;
            idSample = bodySample[0].id;
        } else {
            let errMsg = 'Sample not found: ' + analysis['sample'];
            let daemonKey = analysis['sample'];
            daemon = yield DaemonService.ErrorDeamon(daemon, errMsg, connections.bearerToken);
            return;
        }

        //CERCARE SUBJECTCODE PAZIENTE TRAMITE ID DONOR

        if(donor) {

            let [resSubj, bodyS] = yield request.getAsync({
                uri: basePath + '/subject/' + donor,
                auth: {
                    bearer: connections.bearerToken
                }
            });
            if (resSubj.statusCode !== OK || !bodyS) {
                daemon = yield DaemonService.ErrorDeamon(daemon, resSubj.body, connections.bearerToken);
                return;
            }
            let BodySubject = JSON.parse(bodyS);
 
            if (BodySubject && BodySubject.code) {
                subjectCode = BodySubject.code;
            }
            else {
                let errMsg = 'Patient not found for sample: ' + analysis['sample'];
                daemon = yield DaemonService.ErrorDeamon(daemon, errMsg, connections.bearerToken);
                return;
            }               

        } else {
            let errMsg = 'Patient not found for sample: ' + analysis['sample'];
            daemon = yield DaemonService.ErrorDeamon(daemon, errMsg, connections.bearerToken);
            return;
        }               

        return {
            "donor": donor,
            "subjectCode": subjectCode,
            "biobankCode": biobankCode,
            "idSample": idSample,
            "daemon": daemon
        };
       }),

   UpsertAnalysisNGSforAN: BluebirdPromise.coroutine(function* (analysis, metadata, daemon) {


           let [resAnalysis, bodyA] = yield request.getAsync({
               uri: basePath + '/data?parentSample=' + analysis.idSample + '&parentSubject=' + analysis.donor,
               auth: {
                   bearer: connections.bearerToken
               }
           });
           //CHECK IF EXIST
           if (resAnalysis.statusCode !== OK || !bodyA) {
               daemon = yield DaemonService.ErrorDeamon(daemon, resAnalysis.body, connections.bearerToken);
               return;
           }
           let BodyAnalysis = JSON.parse(bodyA);

           //IF ALREADY EXIST
           if (BodyAnalysis && _.isArray(BodyAnalysis) && BodyAnalysis.length == 1 ) {
               analysis.id = BodyAnalysis[0].id;
               //check if empty or not
                if(BodyAnalysis[0].metadata && BodyAnalysis[0].metadata.fastq_file_path_r1 && BodyAnalysis[0].metadata.fastq_file_path_r1.values && BodyAnalysis[0].metadata.fastq_file_path_r1.values.length > 0) {
                    return {
                        "analysis": analysis,
                        "daemon": daemon
                    }
                }

               //DO UPDATE
               let [resUpdAn, bodyUpdAn] = yield request.putAsync({
                   uri: basePath + '/data/' + analysis.id,
                   auth: {
                       bearer: connections.bearerToken
                    },
                    json: {
                        type: 212, // Analysis
                        owner: 45,
                        metadata: metadata,
                        parentSample: [analysis.idSample],
                        parentSubject: [analysis.donor]
                    }
                });
            
                if (resUpdAn.statusCode !== OK) {
                    let errorString = "Error updarting Analysis for patient: " + analysis.donor + " and sample: " + analysis.isSample + " - " + bodyUpdAn.error.message.details[0].message;
                    logger.log('info', errorString);
                    daemon = yield DaemonService.ErrorDeamon(daemon, errorString, connections.bearerToken);
                    return {
                        "daemon": daemon
                    }
                }
            } 
            else if (BodyAnalysis && _.isArray(BodyAnalysis) && BodyAnalysis.length > 0 && BodyAnalysis.length < 2 ) {
                let errorString = "Found more than one elegible analysis on DB, check patient: " + analysis.donor + " and sample: " + analysis.isSample;
                logger.log('info', errorString);
                daemon = yield DaemonService.ErrorDeamon(daemon, errorString, connections.bearerToken);
                return {
                    "daemon": daemon
                }
            }
              else { 
                
                  let [resCreaAn, bodyCreaAn] = yield request.postAsync({
                      uri: basePath + '/data',
                      auth: {
                          bearer: connections.bearerToken
                      },
                      json: {
                          type: 212, // Analysis
                          owner: 45,
                          metadata: metadata,
                          parentSample: [analysis.idSample],
                          parentSubject: [analysis.donor]
                      }
                  });
      
                  if (resCreaAn.statusCode !== CREATED) {
                      let errorString = "Error creating Analysis for patient: " + analysis.donor + " and sample: " + analysis.idSample + " - " + bodyCreaAn.error.message.details[0].message;
                      logger.log('info', errorString);
                      daemon = yield DaemonService.ErrorDeamon(daemon, errorString, connections.bearerToken);
                      return;
                  }
  
                  analysis.id = bodyCreaAn.id;
              }
  
              return {
                  "analysis": analysis,
                  "daemon": daemon
              };
          }),

     /**
     * @method
     * @name createNGSANALYSIS
     * @description coroutine for import NGS Patients
     */
    createNGSANALYSIS: BluebirdPromise.coroutine(function* (defaultPath, ext, processInfo) {
        logger.log('info', "createNGSANALYSIS");

        var that = this;
        let objInfo = JSON.parse(processInfo.argv[2]);

        var reWritePath = objInfo.reWritePath;
        // logger.log('info', "createNGSANALYSIS objInfo: " + JSON.stringify(objInfo));
        // return;

        let files = utils.getFilesInFolder(defaultPath + '/' + objInfo.folder, ext);
        // logger.log('info', "FILES: " + JSON.stringify(files));

        if (_.isEmpty(files)) {
            logger.log('info', "Invalid or no files loaded");
            return BluebirdPromise.rejected("No Valid files loaded");
        }
        let analysesDataSource = [];

        var fs = require('fs');
 

        var contents = fs.readFileSync(files[0], 'utf8');
        // console.log(contents);


        
        var rows = contents.split('\n');
        var headerArray = rows[0].split('\t');

        rows.shift();

        rows.map(r=>{
            let row = r.split('\t');
            let nr = {};
            headerArray.map((h, i) =>{
                nr[h] = row[i];
            });
            analysesDataSource.push(nr);
        });
        let updated = 0, created = 0, daemons = {};


        const samplesIDs = _.uniq(_.map(analysesDataSource, 'sample')).filter(r => r != "");
    
        yield BluebirdPromise.each(samplesIDs, co.wrap(function* (sampleID, index) {
            // logger.log('info', 'Analysing row: ' + sampleID);

            let analyses = _.filter(analysesDataSource, {'sample': sampleID})
            if (!analyses[0]['sample']) {
                let errorString = 'No SAMPLE CODE provided, row: ' + index;
                let daemon = yield DaemonService.InitializeDeamon(" ", {}, objInfo.executor, process, connections.bearerToken);
                daemon = yield DaemonService.ErrorDeamon(daemon, errorString, connections.bearerToken);
                logger.log('info', errorString);
                return BluebirdPromise.rejected(errorString);
            } 
            
            let daemon = yield DaemonService.InitializeDeamon(sampleID, {}, objInfo.executor, process, connections.bearerToken);
                
                    

            // let probandsID = [];
            daemon.info.totalRows = analyses.length;
            daemon.info.processedRows = 0;
            daemon = yield DaemonService.UpdateDeamon(daemon, connections.bearerToken);

            // yield BluebirdPromise.each(analyses, co.wrap(function* (analysis) {


            //CERCO PAZIENTE E CAMPIONE

            const resSample = yield that.GetPatientAndSampleNGSforAN(analyses[0], daemon);
            analyses[0].biobankCode = resSample.biobankCode;
            analyses[0].subjectCode = resSample.subjectCode;
            analyses[0].donor = resSample.donor;
            analyses[0].idSample = resSample.idSample;
            daemon = resSample.daemon;

            // logger.log('info', 'Can rewrite: ' + reWritePath);
            if(reWritePath) {
                // logger.log('info', 'IF reWritePath IN');

                const resRewrite = yield utils.RewritePathNGS(analyses, daemon);
                if(!resRewrite.analyses) return;
                analyses = resRewrite.analyses;
                daemon = resRewrite.daemon;
            }

            let metadata = utils.composeNSGAnalysisMetadata(analyses);

            
            const resAnalysis = yield that.UpsertAnalysisNGSforAN(analyses[0], metadata, daemon);
            if(!resAnalysis.analysis) return;
            analyses[0] = resAnalysis.analysis;
            daemon = resAnalysis.daemon;

            daemon.info.processedRows += analyses.length;
            daemon.info.notProcessedRows = [];
            daemon = yield DaemonService.SuccessDeamon(daemon, connections.bearerToken);
            

            logger.log('info', "Migrator.createNGSANALYSIS - Analyses for Sample: " + sampleID + " correctly imported.");
 
        }));

        logger.log('info', "Migrator.createNGSANALYSIS - all analyses correctly imported.");

        return { created: 0, updated: 0, notUpdated: 0 };
    })
};

/**
 * @class
 * @name Migrator
 * @description a set of utility methods to migrate data from the legacy MySQL to the latest PostgreSQL 9.4 database
 */
function Migrator(mysqlConn, pgConn, annoPgConn) {
    logger.log('info', "Migrator");

    if (!mysqlConn) mysqlConn = 'mysqlLocal';
    if (!pgConn) pgConn = 'postgresqlLocal';
    if (!annoPgConn) annoPgConn = 'postgresqlLocalAnnotiation';

    logger.log("info",connections[mysqlConn]);
    logger.log("info",connections[pgConn]);



    // this.knexMysql = require('knex')(connections[mysqlConn]);
    // this.knexMysql.select('ID_PRJ').from('PROJECT').then(console.log).catch(console.log);
    this.knexPg = require('knex')(connections[pgConn]);
    this.knexAnnoPg = require('knex')(connections[annoPgConn]);

    this.subjectMap = {};
    this.sampleMap = {};
    // this.knexPg.select('name').from('data_type').then(console.log).catch(console.log);
    //
    this.dataTypeMap = {
        'Patient': 1,
        'Tissue': 2,
        'Fluid': 3,
        'DNA': 4,
        'RNA': 5
    };
}

Migrator.prototype = {

    /**
     * @name migrateProjects
     * @description tool to migrate all the projects
     */
    migrateProjects: function () {

        let knexPg = this.knexPg;

        return this.knexMysql.select('ID_PRJ', 'NAME_PROJECT', 'DESCR_PROJECT').from('PROJECT')
            .orderBy('ID_PRJ')

            .then(function (rows) {
                // console.log(rows);
                return BluebirdPromise.each(rows, function (record) {
                    // console.log(record);
                    return knexPg.returning('id').insert({
                        'name': record.NAME_PROJECT,
                        'description': record.DESCR_PROJECT,
                        'created_at': new Date(),
                        'updated_at': new Date()
                    }).into('project');
                }, { concurrency: 1 });
            });

    },

    /**
     * @method
     * @name migrateAllSubjects
     */
    migrateAllSubjects: function () {
        let that = this;
        return this.knexMysql.select('ID_PRS_DATA').from('PERSONAL_DATA')

            // once you get all the subjects' IDS
            .then(function (rows) {
                // console.log(_.pluck(rows, 'ID_PRS_DATA'));

                // insert each new Subject
                return BluebirdPromise.each(rows, function (subj) {
                    let mysqlSubjId = subj && subj.ID_PRS_DATA;
                    logger.log('info', "Migrator.migrateAllSubject - migrating subject " + mysqlSubjId);
                    return that.migrateCompleteSubject(mysqlSubjId);
                });

            });
    },

    /**
     * @method
     * @name migrateCompleteSubject
     * @param {Integer} mysqlSubjId - the ID of the subject in MySQL
     */
    migrateCompleteSubject: function (mysqlSubjId) {

        let that = this, idSubject;
        return this.migrateSubject(mysqlSubjId)

            .then(function (subjId) {
                idSubject = subjId;
                that.subjectMap[mysqlSubjId] = idSubject;
                return that.migrateNBClinicalData(mysqlSubjId, idSubject);
            })

            .then(function () {
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
    migrateSubject: function (mysqlId) {

        let knexPg = this.knexPg;
        let idSubject;

        let query = this.knexMysql.select('NAME', 'SURNAME', 'BIRTH_DATE', 'ID_SEX', 'CODE', 'ID_PRJ', 'INSERT_DATE', 'DATE_LAST_UPDATE')
            .from('PERSONAL_DATA')
            .leftJoin('PATIENT', 'PERSONAL_DATA.ID_PRS_DATA', 'PATIENT.ID_PRS_DATA')
            .where('PERSONAL_DATA.ID_PRS_DATA', '=', mysqlId);

        logger.log('info', query.toString());

        return query.then(function (rows) {
            let record = rows[0];
            return knexPg.transaction(function (trx) {
                // insert Personal Details
                return knexPg.returning('id').insert({
                    'given_name': record.NAME || " ",
                    'surname': record.SURNAME || " ",
                    'birth_date': formatDate(record.BIRTH_DATE) || '1970-01-01',
                    'created_at': formatDate(record.INSERT_DATE),
                    'updated_at': formatDate(record.DATE_LAST_UPDATE)
                }).into('personal_details').transacting(trx)

                    // insert Subject
                    .then(function (ids) {
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
                    .then(function (ids) {
                        idSubject = ids[0];
                        return knexPg.insert({ 'project_subjects': record.ID_PRJ, 'subject_projects': idSubject })
                            .into('project_subjects__subject_projects').transacting(trx);
                    });
            })
                .then(function () {
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
    migrateNBClinicalData: function (mysqlSubjId, pgSubjId) {
        let knexPg = this.knexPg;

        let query = this.knexMysql.select('NB_CLINICAL_SITUATION.*', 'CLINICAL_PROTOCOL.NAME_CLINICAL_PROTOCOL',
            'NB_HISTOLOGY.DESCR_NB_HISTOLOGY', 'NB_PRIMARY_SITE.DESCR_NB_PRIMARY_SITE')
            .from('NB_CLINICAL_SITUATION')
            .leftJoin('CLINICAL_PROTOCOL', 'NB_CLINICAL_SITUATION.ID_CLINICAL_PROTOCOL', 'CLINICAL_PROTOCOL.ID_CLINICAL_PROTOCOL')
            .leftJoin('NB_HISTOLOGY', 'NB_CLINICAL_SITUATION.ID_NB_HISTOLOGY', 'NB_HISTOLOGY.ID_NB_HISTOLOGY')
            .leftJoin('NB_PRIMARY_SITE', 'NB_CLINICAL_SITUATION.ID_NB_PRIMARY_SITE', 'NB_PRIMARY_SITE.ID_NB_PRIMARY_SITE')
            .where('ID_PATIENT', mysqlSubjId);

        logger.log('debug', query.toString());

        return query.then(function (rows) {

            if (_.isEmpty(rows)) return [null, null];

            let clinSit = rows[0];

            if (!clinSit.ID_NB_REG) return [null, null];

            let payload = {
                type: NB_CLINICAL_SITUATION_POSTGRES_ID,
                parentSubject: [pgSubjId],
                metadata: {
                    italian_nb_registry_id: { value: clinSit.ID_NB_REG },
                    diagnosis_date: { value: formatDate(clinSit.DIAGNOSIS_DATE) },
                    diagnosis_age: { value: clinSit.DIAGNOSIS_AGE, unit: 'month' },
                    clinical_protocol: { value: clinSit.NAME_CLINICAL_PROTOCOL },
                    inss: { value: clinSit.INSS || null },
                    inrgss: { value: clinSit.INRGSS || null },
                    histology: { value: clinSit.DESCR_NB_HISTOLOGY },
                    primary_site: { value: clinSit.DESCR_NB_PRIMARY_SITE },
                    relapse: { value: clinSit.RELAPSE || null },
                    relapse_date: { value: formatDate(clinSit.RELAPSE_DATE) },
                    relapse_type: { value: clinSit.RELAPSE_TYPE || null },
                    last_follow_up_date: { value: formatDate(clinSit.LAST_FOLLOW_UP_DATE) },
                    clinical_follow_up_status: { value: clinSit.CLINICAL_FOLLOW_UP_STATUS || null },
                    ploidy: { value: clinSit.PLOIDY },
                    mycn_status: { value: clinSit.MYCN_STATUS || null },
                    event_overall: { value: clinSit.EVENT_OVERALL || 'N.D.' },
                    event_progfree: { value: clinSit.EVENT_PROGFREE || 'N.D.' },
                    survival_overall: { value: clinSit.SURVIVAL_OVERALL, unit: 'day' },
                    survival_progfree: { value: clinSit.SURVIVAL_PROGFREE, unit: 'day' }
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
            .spread(function (res, body) {

                if (res && res.statusCode !== CREATED) {
                    logger.log('error', res && res.request && res.request.body);
                    return BluebirdPromise.rejected("Migrator.createNBClinicalData: clinical data was not correctly created for MySQL subject code " + mysqlSubjId);
                }
                return true;

            });

    },

    /**
     * @method
     * @name migrateSamples
     * @description migrate primary samples
     */
    migrateSamples: function (mysqlSubjId) {

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
            .whereNull('ID_PARENT_SAMPLE').whereIn('SAMPLE.ID_SAMPLE_TYPE', ['TIS', 'FLD']).andWhere('ID_PATIENT', mysqlSubjId)

            .then(function (rows) {
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

            .then(function (tsRows) {
                // console.log("Migrator.migrateSamples - got these tumour statuses: ");
                // console.log(tsRows);
                // console.log("Migrator.migrateSamples - samples: ");
                // console.log(samples);

                // save each (primary) sample
                return BluebirdPromise.each(samples, function (sample, index) {
                    let tumourStatus = _.findWhere(tsRows, { ID_SAMPLE: sample.ID_SAMPLE });
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
    createPrimary: function (sample, idSubj, tumourStatus) {
        if (!sample.BIT_CODE) {
            logger.log('info', "Migrator.migrateSampleDerivatives - sample misses required info: " + sample.ID_SAMPLE);
            return;
        }
        // console.log("Migrator.createPrimary - idSubject: " + idSubj);
        let that = this;
        let knexPg = this.knexPg;
        let sampleType, sampleTypeName, idPrimarySample, ts;

        // change FAT to ADIPOUS TISSUE
        if (sample.DESCR_BIT_TISSUE_NAME === 'FAT') sample.DESCR_BIT_TISSUE_NAME = 'ADIPOUS TISSUE';

        let metadata = {
            arrival_date_mb: { value: formatDate(sample.BM_ARRIVAL_DATE) },
            sample_name: { value: sample.SAMPLE_NAME || null },
            sample_codification: { value: sample.DESCR_BIT_TISSUE_NAME || 'UNDETERMINED' },
            pathology: { value: sample.DESCR_BIT_HISTOPATHOLOGY || 'NONE' },
            city: { value: sample.CITY },
            hospital: { value: sample.DESCR_HOSP || null },
            unit: { value: sample.UNIT }
        };

        switch (sample.ID_SAMPLE_TYPE) {
            case 'TIS':
                metadata.arrival_code = { value: sample.ARRIVAL_CODE || null };
                metadata.arrival_date_pa = { value: formatDate(sample.AP_ARRIVAL_DATE) };
                metadata.size_x = { value: sample.SIZE_FIRST, unit: 'mm' };
                metadata.size_y = { value: sample.SIZE_SECOND, unit: 'mm' };
                metadata.size_z = { value: sample.SIZE_THIRD, unit: 'mm' };
                metadata.tumour_cellularity = { value: sample.CELLULARITY, unit: '%' };
                sampleTypeName = 'Tissue';
                break;
            case 'FLD':
                metadata.sampling_date = { value: formatDate(sample.AP_ARRIVAL_DATE) };
                metadata.quantity = { value: sample.QUANTITY, unit: 'ml' };
                sampleTypeName = 'Fluid';
        }

        // if the tumour status is not empty
        if (!_.isEmpty(tumourStatus)) {
            let jsonSchema = JSON.parse(tumourStatus.JSON_SCHEMA);
            metadata.tumour_status = { value: jsonSchema.body[0].content[0].instances[0].value };
        }
        else {
            try {
                ts = sample.NOTES.split(/  +/)[0].split(':')[1].trim();
            }
            catch (err) {
                logger.log('warn', err.message);
                ts = null;
            }
            // console.log(ts);
            metadata.tumour_status = { value: ts || null };
            if (allowedTumourStatuses.indexOf(ts) > -1) {
                metadata.tumour_status = { value: ts };
            }
            else if (ts === 'POST-CHEMIO' || ts === 'POST CHEMIO') {
                metadata.tumour_status = { value: 'POST-CHEMO' };
            }
            else {
                metadata.tumour_status = { value: null };
            }
        }

        // console.log("Migrator.createPrimary - metadata: " + metadata);
        return knexPg.select('id').from('data_type').where('name', sampleTypeName)

            .then(function (rows) {
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
                            donor: [idSubj],
                            metadata: metadata
                        }
                    });

                }
            })

            .spread(function (res, body) {
                // console.log("Migrator.createPrimary - response is: " + res.statusCode);
                if (res && res.statusCode !== CREATED) {
                    logger.log('error', res && res.request && res.request.body);
                    return BluebirdPromise.rejected("Migrator.createPrimary: sample was not correctly created for biobank code " + sample.BIT_CODE);
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

            .then(function () {
                return that.migrateNucleicDerivatives(sample.ID_SAMPLE, idSubj);
            });

    },

    /**
     * @method
     * @name migratePlasmaSamples
     */
    migratePlasmaSamples: function (mysqlSampleId, idSubj, sampleType) {

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
            .where('ID_PARENT_SAMPLE', mysqlSampleId).andWhere('DESCR_BIT_TISSUE_NAME', 'PLASMA')

            .then(function (rows) {
                // console.log("Migrator.migratePlasmaSamples: got these samples: " + rows);

                return BluebirdPromise.each(rows, function (sample) {
                    let metadata = {
                        arrival_date_mb: { value: formatDate(sample.BM_ARRIVAL_DATE) },
                        sample_name: { value: sample.SAMPLE_NAME || null },
                        sample_codification: { value: sample.DESCR_BIT_TISSUE_NAME || 'UNDETERMINED' },
                        pathology: { value: sample.DESCR_BIT_HISTOPATHOLOGY || 'NONE' },
                        city: { value: sample.CITY },
                        hospital: { value: sample.DESCR_HOSP || null },
                        unit: { value: sample.UNIT },
                        sampling_date: { value: formatDate(sample.AP_ARRIVAL_DATE) },
                        quantity: { value: sample.QUANTITY || 0, unit: 'ml' }
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
                            donor: [idSubj],
                            parentSample: [that.sampleMap[mysqlSampleId]]
                        }
                    })
                        .spread(function (res, body) {
                            if (res.statusCode !== CREATED) {
                                // console.log(res && res.request && res.request.body);
                                return BluebirdPromise.rejected("Migrator.migratePlasmaSamples - sample was not correctly created for sample " + sample.BIT_CODE);
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
    migrateNucleicDerivatives: function (mysqlSampleId, idSubj) {
        let that = this;
        let metadata, derivatives;

        logger.log('info', "Migrator.migrateSampleDerivatives - here we are");

        return this.knexMysql.select('SAMPLE.ID_SAMPLE', 'BIT_CODE', 'ARRIVAL_CODE', 'ID_SAMPLE_TYPE', 'QUANTITY', 'EXTRACTION_DATE', 'CONCENTRATION')
            .from('SAMPLE')
            .whereIn('ID_SAMPLE_TYPE', ['DNA', 'RNA']).andWhere('ID_PARENT_SAMPLE', mysqlSampleId)

            .then(function (rows) {
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

            .then(function (qcRows) {
                let jsonSchema;
                // console.log("Migrator.migrateNucleicDerivatives - qcRows: ");
                // console.log(qcRows);
                return BluebirdPromise.each(derivatives, function (derivative) {
                    if (!derivative.BIT_CODE || derivative.BIT_CODE === "NO" || !derivative.EXTRACTION_DATE) {
                        // console.log ("Migrator.migrateSampleDerivatives - sample misses required info: " + derivative.ID_SAMPLE);
                        // TODO log missing samples
                        return;
                    }

                    let quality, mappedDerivative;
                    let qc = _.findWhere(qcRows, { 'ID_SAMPLE': derivative.ID_SAMPLE }) || {};

                    logger.log('info', "Migrator.migrateNucleicDerivatives - qc: ");
                    // console.log(qc);
                    if (derivative.QUANTITY === null || derivative.QUANTITY === undefined) {
                        logger.log('warn', "Migrator.migrateSampleDerivatives - sample " + derivative.ID_SAMPLE + " has no quantity information");
                    }

                    metadata = {
                        arrival_code: { value: derivative.ARRIVAL_CODE || null },
                        sampling_date: { value: formatDate(derivative.EXTRACTION_DATE) },
                        quantity: { value: derivative.QUANTITY || 0, unit: 'μg' },
                        concentration: { value: derivative.CONCENTRATION, unit: 'ng/μl' }
                    };

                    if (!_.isEmpty(qc)) {
                        jsonSchema = JSON.parse(qc.JSON_SCHEMA);
                        quality = allowedQualities.indexOf(jsonSchema.body[0].content[0].instances[0].value) > -1 ?
                            jsonSchema.body[0].content[0].instances[0].value : allowedQualities[allowedQualities.length - 1];
                        metadata.quality = { value: quality };
                        metadata.kit_type = { value: jsonSchema.body[0].content[1].instances[0].value };
                        metadata._260_280 = { value: jsonSchema.body[0].content[2].instances[0].value };
                        metadata._260_230 = { value: jsonSchema.body[0].content[3].instances[0].value };
                    }

                    let payload = {
                        type: that.dataTypeMap[derivative.ID_SAMPLE_TYPE],
                        biobank: 1,
                        biobankCode: derivative.BIT_CODE,
                        donor: [idSubj],
                        parentSample: [that.sampleMap[mysqlSampleId]],
                        metadata: metadata
                    };

                    // retrieve data files associated to the QC
                    return that.getAndFormatDataFiles(qc.ID_FCOLL)

                        .then(function (files) {

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

                        .spread(function (res, body) {
                            if (res.statusCode !== CREATED) {
                                logger.log('error', res.statusCode);
                                logger.log('error', res && res.request && res.request.body);
                                return BluebirdPromise.rejected("Migrator.migrateSampleDerivatives - sample was not correctly created for biobank code " + derivative.BIT_CODE);
                            }
                            // console.log(body);
                            return {
                                idSampleMysql: derivative.ID_SAMPLE,
                                idSamplePg: body.id,
                                type: body.type
                            };
                        })

                        .then(function (sampleObj) {

                            mappedDerivative = sampleObj;
                            logger.log('info', 'Migrator.migrateNucleicDerivatives - derivative migrated');
                            logger.log('info', sampleObj);

                            if (sampleObj.type === that.dataTypeMap.RNA) {
                                logger.log('info', "Migrator.migrateSampleDerivatives - sample is RNA migrating Microarrays...");
                                logger.log('info', sampleObj);
                                return that.migrateMicroarrays(sampleObj.idSampleMysql, sampleObj.idSamplePg, idSubj);
                            }

                        })

                        .then(function () {

                            logger.log('info', "Migrator.migrateSampleDerivatives - ready to migrate aliquot deliveries");
                            return that.migrateAliquotDelivery(mappedDerivative.idSampleMysql, mappedDerivative.idSamplePg, idSubj);

                        })

                        .then(function () {

                            logger.log('info', "Migrator.migrateSampleDerivatives - ready to migrate ALK Reports");
                            return that.migrateAlkReport(mappedDerivative.idSampleMysql, mappedDerivative.idSamplePg, idSubj);

                        });

                });

            })

            .then(function (samples) {

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

    migrateCGH: function (folder, ext, processInfo) {

        return coroutines.migrateCGH(folder, ext, processInfo)
            .catch(function (err) {
                throw err;
            });
    },

    /**
     * @method
     * @name migrateBioAn
     * @description migrates all the CGH files in a given folder and saves them on XTENS database
     * @param{string} folder - the absolute path of the folder containing all the CGH files
     * @param{string} ext - the extension of the files (XLSX, XLS)
     */

    migrateBioAn: function (folder, ext, processInfo) {

        return coroutines.migrateBioAn(folder, ext, processInfo)
            .catch(function (err) {
                throw err;
            });
    },

    /**
     * @method
     * @name migrateNKCells
     * @description migrates all the CGH files in a given folder and saves them on XTENS database
     * @param{string} folder - the absolute path of the folder containing all the CGH files
     * @param{string} ext - the extension of the files (XLSX, XLS)
     */

    migrateNKCells: function (folder, ext, processInfo) {

        return coroutines.migrateNKCells(folder, ext, processInfo)
            .catch(function (err) {
                throw err;
            });
    },

    /**
     * @method
     * @name importCNBInfo
     * @description import all the Clinical Information files in a given folder and saves them on XTENS database
     * @param{string} folder - the absolute path of the folder containing all the Clinical Information files
     * @param{string} ext - the extension of the files (XLSX, XLS)
     */
    importCNBInfo: function (folder, ext) {

        return coroutines.importCNBInfo(folder, ext)
            .catch(function (err) {
                throw err;
            });
    },

    /**
     * @method
     * @name migrateVCF
     * @description import all the vcf files in a given folder and saves them on XTENS database
     * @param{string} folder - the absolute path of the folder containing all the VCF files
     * @param{string} ext - the extension of the files (VCF)
     */
    migrateVCF: function (folder, ext, process) {
        let knexAnno = this.knexAnnoPg;
        return coroutines.migrateVCF(folder, ext, process, knexAnno)
            .catch(function (err) {
                throw err;
            });
    },

     /**
     * @method
     * @name migrateNGSPATIENTS
     * @description imports xlsx files in a given folder and saves rows on XTENS database
     * @param{string} folder - the absolute path of the folder containing xlsx file
     * @param{string} ext - the extension of the files (XLSX)
     */

    migrateNGSPATIENTS: function (folder, ext, processInfo) {

        return coroutines.migrateNGSPATIENTS(folder, ext, processInfo)
            .catch(function (err) {
                throw err;
            });
    },

         /**
     * @method
     * @name createNGSANALYSIS
     * @description imports xlsx files in a given folder and saves rows on XTENS database
     * @param{string} folder - the absolute path of the folder containing xlsx file
     * @param{string} ext - the extension of the files (XLSX)
     */

    createNGSANALYSIS: function (folder, ext, processInfo) {

        return coroutines.createNGSANALYSIS(folder, ext, processInfo)
            .catch(function (err) {
                throw err;
            });
    },

    /**
     * @method
     * @name getAndFormatDataFiles
     * @description retrieve associated data files from the old database and format them to be put on the new one
     * @param{integer} idFileCollMysql
     */
    getAndFormatDataFiles: function (idFileCollMysql) {

        // retrieve data files associated to the QC
        return this.knexMysql.select('URI').from('FILE_XTENS')
            .where('ID_FCOLL', idFileCollMysql).andWhere('FILE_TYPE', "DATA")

            .then(function (files) {
                if (!_.isEmpty(files)) {
                    logger.log('info', "Migrator.migrateSampleDerivatives - found files");
                    logger.log('info', files);
                    files = _.map(files, file => {
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
    migrateMicroarrays: function (idSampleMysql, idSamplePg, idSubjPg) {

        let that = this, rawId, mas5Id;
        logger.log('info', "Migrator.migrateMicroarrays: here we are");

        let query = this.knexMysql.select('ID_DATA', 'ID_SAMPLE', 'ID_FCOLL', 'DESCR_DATA', 'JSON_SCHEMA')
            .from('DATA')
            .leftJoin('SAMPLE_COLLECTION', 'SAMPLE_COLLECTION.ID_SCOLL', 'DATA.ID_SCOLL')
            .leftJoin('SAMPLE_SCOLL', 'SAMPLE_SCOLL.ID_SCOLL', 'SAMPLE_COLLECTION.ID_SCOLL')
            .whereIn('DESCR_DATA', [MICROARRAY_RAW, MICROARRAY_MAS5, MICROARRAY_NB])
            .andWhere('ID_SAMPLE', idSampleMysql);

        logger.log('debug', query.toString());

        return query.then(function (microarrayData) {

            let logOut = _.map(_.cloneDeep(microarrayData), datum => {
                return _.omit(datum, 'JSON_SCHEMA');
            });
            logger.log('info', logOut);

            if (_.isEmpty(microarrayData)) return;
            else logger.log('info', "Migrator.migrateMicroarrays: Microarray data found!!");

            let microarrayRaw = _.findWhere(microarrayData, { 'DESCR_DATA': MICROARRAY_RAW });
            let microarrayMas5 = _.findWhere(microarrayData, { DESCR_DATA: MICROARRAY_MAS5 });

            logger.log('debug', 'Microarray Raw:' + microarrayRaw);

            if (!microarrayRaw) {
                return;
            }
            let jsonSchema = JSON.parse(microarrayRaw.JSON_SCHEMA);

            return that.getAndFormatDataFiles(microarrayRaw.ID_FCOLL)

                .then(function (files) {

                    return request.postAsync({
                        uri: basePath + '/data',
                        auth: {
                            bearer: connections.bearerToken
                        },
                        json: {
                            type: 11, // Microarray RAW
                            metadata: {
                                platform: { value: jsonSchema.body[0].content[0].instances[0].value }
                            },
                            parentSubject: [idSubjPg],
                            parentSample: [idSamplePg],
                            files: files
                        }
                    });

                })

                .spread(function (res, createdRaw) {

                    logger.log('debug', 'Migrator.migrateMicroarrays: created new raw: ' + createdRaw);

                    rawId = createdRaw.id;
                    if (!microarrayMas5) return;
                    return that.getAndFormatDataFiles(microarrayMas5.ID_FCOLL);
                })

                .then(function (files) {

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
                                normalization_details: { value: jsonSchema.body[0].content[0].instances[0].value }
                            },
                            parentSubject: [idSubjPg],
                            parentData: [rawId],
                            files: files
                        }
                    });
                })

                .spread(function (res, createdMas5) {
                    logger.log('debug', 'Migrator.migrateMicroarrays: created new mas5: ' + createdMas5);

                    let microarrayReport = _.findWhere(microarrayData, { DESCR_DATA: MICROARRAY_NB });
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
                        parentSubject: [idSubjPg],
                        parentData: createdMas5.id
                    };
                    logger.log('info', "Migrator.migrateMicroarrays: payload for Microarray report is: ");
                    logger.log('info', payload);

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
    migrateAliquotDelivery: function (idSampleMysql, idSamplePg, idSubjPg) {

        let jsonSchema;
        let query = this.knexMysql.select('ID_DATA', 'ID_SAMPLE', 'ID_FCOLL', 'DESCR_DATA', 'JSON_SCHEMA')
            .from('DATA')
            .leftJoin('SAMPLE_COLLECTION', 'SAMPLE_COLLECTION.ID_SCOLL', 'DATA.ID_SCOLL')
            .leftJoin('SAMPLE_SCOLL', 'SAMPLE_SCOLL.ID_SCOLL', 'SAMPLE_COLLECTION.ID_SCOLL')
            .whereIn('DESCR_DATA', [ALIQUOT_DELIVERY])
            .andWhere('ID_SAMPLE', idSampleMysql);

        logger.log('debug', query.toString());

        return query.then(function (aliquotDeliveryData) {
            return BluebirdPromise.map(aliquotDeliveryData, function (ad) {

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
                        parentSubject: [idSubjPg],
                        parentSample: [idSamplePg]
                    }
                });

            })

                .then(function (inserted) {
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
    migrateAlkReport: function (idSampleMysql, idSamplePg, idSubjPg) {

        let jsonSchema;
        let query = this.knexMysql.select('ID_DATA', 'ID_SAMPLE', 'ID_FCOLL', 'DESCR_DATA', 'JSON_SCHEMA')
            .from('DATA')
            .leftJoin('SAMPLE_COLLECTION', 'SAMPLE_COLLECTION.ID_SCOLL', 'DATA.ID_SCOLL')
            .leftJoin('SAMPLE_SCOLL', 'SAMPLE_SCOLL.ID_SCOLL', 'SAMPLE_COLLECTION.ID_SCOLL')
            .where('DESCR_DATA', ALK_MUTATION).andWhere('ID_SAMPLE', idSampleMysql);

        logger.log('debug', query.toString());

        return query.then(function (alkData) {

            if (_.isEmpty(alkData)) {
                return;
            }

            return BluebirdPromise.each(alkData, function (alk) {

                jsonSchema = JSON.parse(alk.JSON_SCHEMA);

                return request.postAsync({
                    uri: basePath + '/data',
                    auth: {
                        bearer: connections.bearerToken
                    },
                    json: {
                        type: 15, // ALK mutation
                        metadata: {
                            exon_20: { values: _.pluck(jsonSchema.body[0].content[0].content[0].instances, 'value') },
                            exon_21: { values: _.pluck(jsonSchema.body[0].content[1].content[0].instances, 'value') },
                            exon_22: { values: _.pluck(jsonSchema.body[0].content[2].content[0].instances, 'value') },
                            exon_23: { values: _.pluck(jsonSchema.body[0].content[3].content[0].instances, 'value') },
                            exon_24: { values: _.pluck(jsonSchema.body[0].content[4].content[0].instances, 'value') },
                            exon_25: { values: _.pluck(jsonSchema.body[0].content[5].content[0].instances, 'value') },
                            result: { value: jsonSchema.body[1].content[0].instances[0].value },
                            germline_mutation: { value: jsonSchema.body[2].content[0].instances[0].value }
                        },
                        parentSubject: [idSubjPg],
                        parentSample: [idSamplePg]
                    }
                })

                    .spread(function (res, body) {
                        if (res.statusCode !== CREATED) {
                            logger.log('error', 'Migrator.migrateAlkReport - ALK Mutation Report not correctly created');
                        }
                    });

            })

                .then(function (inserted) {
                    logger.log('info', 'Migrator.migrateAlkReport - done!');
                    logger.log('info', 'Inserted ' + inserted.length + 'record(s)');
                    return true;
                });

        });


    }


};

module.exports = Migrator;
