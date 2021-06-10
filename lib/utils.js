/* eslint-disable camelcase */
/**
 * @author Massimiliano Izzo
 */
/* jshint node:true */
/* jshint esnext: true */
"use strict";

let _ = require("lodash");
let fs = require("fs");
let path = require("path");
let xlsx = require("xlsx");
let metadataFieldNameNotAllowedCharset = /[^A-Za-z_$0-9:]/g;
let floatFields = ['Threshold', 'Centralization (legacy) Threshold'];
let integerFields = ['Centralization (legacy) Bin Size'];
let booleanFields = ['Fuzzy Zero', 'GC Correction', 'Centralization (legacy)', 'Diploid Peak Centralization',
    'Manually Reassign Peaks', 'Combine Replicates (Intra Array)', 'Combine Replicates (Inter Array)',
    'Genomic Boundary', 'Show Flat Intervals'];
let enumFields = ['genomic profile'];
let moment = require('moment-timezone');
var loggerGen = require('./../logger.js');
const logger = loggerGen();
let config = require('../config/connections.js');
const BluebirdPromise = require('bluebird');
const co = require('co');
const DaemonService = require("./DaemonService.js");

/**
 * @method
 * @name getFilesInFolder
 * @param{string} folder - the folder where you search for files
 * @param{string} ext - extension to be matched
 * @param{boolean} isDeep - if true search subfolders as well
 * @return{Array} files - the list of files in the directory
 */
function getFilesInFolder (folder, ext, isDeep) {
    let res = [];
    if (process.env.NGSPrefixDebugSourceFilesPath && process.env.NGSPrefixDebugSourceFilesPath !== '') {
        folder = process.env.NGSPrefixDebugSourceFilesPath + folder;
    }
    fs.readdirSync(folder).forEach(function (file) {
        logger.log('info', file);
        file = folder + "/" + file;
        let stat = fs.statSync(file);

        if (stat && stat.isDirectory()) {
            if (isDeep) {
                res = res.concat(getFilesInFolder(file));
            }
        } else if (!ext || (_.isArray(ext) ? ext.indexOf(path.extname(file)) > -1 : path.extname(file) === ext)) {
            logger.log('info', "Pushing file: " + file);
            res.push(file);
        }
    });
    return res;
}

/**
 * @method
 * @name parseFileNameVcf
 * @param{string} file - filename
 * @param{string} folder - the folder where you search for files
 * @param{string} contest - the contest of regex validation
 */
function parseFileName (file, folder, contest) {
    let res = {};
    if (!regexExpValidation[contest]) {
        res.error = "Server Error: regexExpValidation " + contest + "not found.";
        return res;
    }
    const vcfRegexExp = regexExpValidation[contest];
    let fileName = file.split(folder + '/')[1];
    // let daemon = yield DaemonService.InitializeDeamon(fileName , {}, objInfo.executor, processInfo, objInfo.bearerToken);

    if (vcfRegexExp.test(fileName)) {
        switch (contest) {
            case "vcf":
                var fileNameSplitted = fileName.split('_');
                res.fileName = fileName;
                res.codePatient = fileNameSplitted[0];
                // res.idFamily = fileNameSplitted[1];
                res.tissueID = fileNameSplitted[1].split('-');
                res.tissueType = res.tissueID[0];
                if (res.tissueID.length > 2) {
                    res.tissueCode = res.tissueID[1];
                    for (var i = 2; i < res.tissueID.length; i++) {
                        res.tissueCode = res.tissueCode + '-' + res.tissueID[i];
                    }
                } else {
                    res.tissueCode = res.tissueID[1];
                }
                res.machine = fileNameSplitted[2];
                res.capture = fileNameSplitted[3].split('.vcf')[0];
                break;
            case "cgh":
                // TODO:
                break;
            default:
                res.fileName = fileName;
        }
    } else {
        res.error = true;
    }
    return res;
}

/**
 * @method
 * @name formatMetadataFieldName
 * @param{string} - name: unformatted metadata field name
 * @return{string} - formatted metadata field name
 */
function formatMetadataFieldName (name) {
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
function composeCGHProcessedMetadata (cghMetadataArr) {
    let res = {};
    _.each(cghMetadataArr, function (metadatumArr) {
        if (metadatumArr[0] === 'Window Size') {
            let value = parseFloat(metadatumArr[1].match(/\d+/)[0]);
            console.log("Window Size value: " + value);
            if (!_.isNaN(value)) {
                res[formatMetadataFieldName(metadatumArr[0])] = {
                    value: value,
                    unit: metadatumArr[1].replace(/\d/g, '').toLowerCase()
                };
            } else {
                throw new Error("utils.composeCGHProcessedMetadata - Could not correctly parse Window Size");
            }
        } else if (floatFields.indexOf(metadatumArr[0]) > -1) {
            res[formatMetadataFieldName(metadatumArr[0])] = { value: parseFloat(metadatumArr[1]) };
        } else if (integerFields.indexOf(metadatumArr[0]) > -1) {
            res[formatMetadataFieldName(metadatumArr[0])] = { value: parseInt(metadatumArr[1]) };
        } else if (booleanFields.indexOf(metadatumArr[0]) > -1) {
            res[formatMetadataFieldName(metadatumArr[0])] = {
                value: metadatumArr[1] === 'ON'
            };
        } else if (enumFields.indexOf(metadatumArr[0].toLowerCase()) > -1) {
            let scaTypeRef = ["Frequent", "Not Frequent", "Frequent - Not Frequent"];
            if (metadatumArr[1].toLowerCase().indexOf('nca') > -1) {
                res["type"] = { value: 'NCA' };
            } else if (metadatumArr[1].toLowerCase().indexOf('flat') > -1 || metadatumArr[1].toLowerCase().indexOf('piatto') > -1) {
                res["type"] = { value: 'Flat Profile' };
            } else if (metadatumArr[1].toLowerCase().indexOf('no') > -1 || metadatumArr[1].toLowerCase().indexOf('result') > -1 || metadatumArr[1].toLowerCase().indexOf('failure') > -1) {
                res["type"] = { value: 'No Result' };
            } else if (metadatumArr[1].toLowerCase().indexOf('sca') > -1 && metadatumArr[2]) {
                res["type"] = { value: 'SCA' };
                if (metadatumArr[2].indexOf(",") || metadatumArr[2].indexOf("-") > 0) {
                    metadatumArr[2] = scaTypeRef[2];
                } else if (metadatumArr[2].indexOf("Not Frequent")) {
                    metadatumArr[2] = scaTypeRef[1];
                } else {
                    metadatumArr[2] = scaTypeRef[0];
                }

                res["sca_type"] = { value: metadatumArr[2] };
            } else {
                res["sca_type"] = { value: null };
            }
        } else {
            res[formatMetadataFieldName(metadatumArr[0])] = { value: metadatumArr[1] };
        }
    });
    console.log(res);
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
function composeCNVMetadata (cnletr) {
    if (cnletr.length < 12 || !cnletr[1]) {
        return;
    }
    let metadata = {};
    metadata.chr = { value: cnletr[1] };
    let cytobands = cnletr[2].split('-').map(Function.prototype.call, String.prototype.trim);
    metadata.cytoband_start = { value: cytobands[0] };
    // if there is only a band, stop band is just the same start band
    let cytobandStop = cytobands.length > 1 ? cytobands[1] : cytobands[0];
    metadata.cytoband_stop = { value: cytobandStop };
    metadata.start = { value: cnletr[3] };
    metadata.stop = { value: cnletr[4] };
    metadata._probes = { value: cnletr[5] };
    let ampl = parseFloat(cnletr[6].toString().replace(',', '.'));
    metadata.is_amplification = { value: ampl > 0 };
    metadata.amplification = { value: ampl };
    let del = parseFloat(cnletr[7].toString().replace(',', '.'));
    metadata.is_deletion = { value: del < 0 };
    metadata.deletion = { value: del };
    metadata.pval = { value: Number(cnletr[8].toString().replace(",", '.')) };
    let geneNames = cnletr[9] && cnletr[9].split(",").map(Function.prototype.call, String.prototype.trim);
    metadata.gene_name = { values: geneNames || [] };
    let mirnas = cnletr[11] && cnletr[11].split(",").map(Function.prototype.call, String.prototype.trim);
    metadata.mirna = { values: mirnas || [] };
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
function composeCGHMetadata (filePath) {
    let fileName = filePath.split("/")[filePath.split("/").length - 1];
    let sampleCode = fileName.split(".")[0];
    console.log(sampleCode);
    let workbook = xlsx.readFile(filePath);
    let worksheet = workbook.Sheets[workbook.SheetNames[0]];
    let range = xlsx.utils.decode_range(worksheet['!ref']);
    //  console.log(range);
    let c, r, firstCellInRow, cell, cellElems;
    let acghProcessedFields = [];
    for (r = range.s.r; r <= range.e.r; r++) {
        firstCellInRow = worksheet[xlsx.utils.encode_cell({ c: 0, r: r })];
        // console.log(firstCellInRow);
        if (firstCellInRow && firstCellInRow.v && firstCellInRow.v.split) {
            if (firstCellInRow && firstCellInRow.v === 'AberrationNo') {
                break;
            }
            cellElems = firstCellInRow.v.split(':').map(Function.prototype.call, String.prototype.trim);
            // console.log("cellElems: ", cellElems);
            acghProcessedFields.push(cellElems);
        } else continue;
    }
    let metadataCGHProcessed = composeCGHProcessedMetadata(acghProcessedFields);

    // Extract metadataGenomicProfile from metadataCGHProcessed
    let type = metadataCGHProcessed['type'];
    let scaType = metadataCGHProcessed['sca_type'];
    let metadataGenomicProfile = { type, scaType };

    delete metadataCGHProcessed['type'];
    delete metadataCGHProcessed['sca_type'];

    // Parse CNV Field Names - NOT USED now
    let cnvFieldNames = {};
    for (c = range.s.c; c <= range.e.c; c++) {
        cnvFieldNames[worksheet[xlsx.utils.encode_cell({ c: c, r: r })].v] = c;
    }
    // console.log(cnvFieldNames);

    let cnletr = [];
    // for each row containing a CNV record
    for (r = r + 1; r <= range.e.r; r++) {
        cellElems = [];
        // put all values in an array
        for (c = range.s.c; c <= range.e.c; c++) {
            cell = worksheet[xlsx.utils.encode_cell({ c: c, r: r })];
            if (cell) {
                cellElems.push(cell.v);
            } else {
                cellElems.push(null);
            }
        }
        // console.log(cellElems);
        // and compose CNV metadata for that record (i.e. array);
        if (!isNaN(cellElems[0]) && cellElems[1] && cellElems[1].indexOf('chr') >= 0) {
            cnletr.push(composeCNVMetadata(cellElems));
        }
        // console.log(_.omit(cnvMetadata, ['gene_name', 'mirna']));
    }
    return {
        sampleCode: sampleCode,
        acghProcessed: metadataCGHProcessed,
        cnletr: _.compact(cnletr),
        genProfile: metadataGenomicProfile
    };
}

/**
 * @method
 * @name composeCNBInfoMetadata
 * @description update a Nb Clinical information data from an Excel file to the PostgreSQL database
 * @param{object} - patient: clinical information
 * @return{object} - object with metadata structure
 *
 */

function composeCNBInfoMetadata (patient) {
    let clinicalStatus, mycn, relapse, hysto, primit, ploidy, rel, evProg, evOver, inss, inrg, protocol, maxRes;

    let dgnDate = moment(patient['DATA_DG'], "MM/DD/YYYY");
    //  let altraDate = moment(patient['DATA_DG']);
    //  console.log(dgnDate.format('YYYY-MM-DD'),altraDate.format('YYYY-MM-DD'));
    let relDate = patient['DATA_RECIDIVAPM'] ? moment(patient['DATA_RECIDIVAPM'], "MM/DD/YYYY") : null;

    let FuDate = patient['DATA_FU_CLINICO'] ? moment(patient['DATA_FU_CLINICO'], "MM/DD/YYYY") : null;
    let daySurvOver = FuDate ? FuDate.diff(dgnDate, 'days') : null;
    let daySurvProg = patient['DATA_RECIDIVAPM'] ? relDate.diff(dgnDate, 'days') : daySurvOver;

    ploidy = patient['GDE_DNA_INDEX'] && patient['GDE_DNA_INDEX'] !== "-9922" ? patient['GDE_DNA_INDEX'] : null;

    evProg = patient['DATA_RECIDIVAPM'] ? "YES" : "NO";

    evOver = patient['D_STATO_FU_CLINICO'] ? patient['D_STATO_FU_CLINICO'] ? patient['D_STATO_FU_CLINICO'].toLowerCase().indexOf('deceduto') > -1 ? "DECEASED" : "ALIVE" : "N.D." : "N.D.";

    rel = patient['DATA_RECIDIVAPM'] ? "YES" : "NO";

    inss = patient['D_STADIO_INSS'] ? patient['D_STADIO_INSS'] === "Non applicabile" ? "N.A." : patient['D_STADIO_INSS'] : null;
    inss = inss && inss !== "Non so" ? inss.replace("Stadio ", "").toUpperCase() : null;

    inrg = patient['D_STADIO_INRG'] ? patient['D_STADIO_INRG'] === "Non applicabile" ? "N.A." : patient['D_STADIO_INRG'] : null;
    inrg = inrg && inrg !== "Non so" ? inrg.replace("Stadio ", "").toUpperCase() : null;

    switch (patient['D_STATO_FU_CLINICO']) {
        case "Vivo in RC per NB/GN":
        case "1 Vivo in RC  per NB/GN":
            clinicalStatus = "ALIVE - COMPLETE REMISSION";
            break;
        case "Vivo con malattia residua stabile per NB/GN":
        case "2 Vivo con malattia residua stabile per NB/GN":
            clinicalStatus = "ALIVE - RESIDUAL DISEASE";
            break;
        case "Vivo con malattia attiva (induzione, recidiva, progressione) per NB":
        case "3 Vivo con malattia attiva (induzione, recidiva, progressione) per NB":
            clinicalStatus = "ALIVE - ACTIVE DISEASE";
            break;
        case "Vivo in RC per NB/GN e secondo tumore attivo":
        case "5 Vivo in RC per NB/GN e secondo tumore attivo":
            clinicalStatus = "ALIVE - SECOND TUMOUR ACTIVE (Different from NB)";
            break;
        case "Vivo in doppia RC per NB/GN e secondo tumore":
            clinicalStatus = "ALIVE - DOUBLE COMPLETE REMISSION AND SECOND TUMOUR (Different from NB)";
            break;
        case "Deceduto per NB":
        case "7 Deceduto per NB":
            clinicalStatus = "DEAD FOR DISEASE";
            break;
        case "Deceduto per tossicita":
        case "8 Deceduto per tossicita":
            clinicalStatus = "DEAD FOR TOXICITY";
            break;
        case "Deceduto per altre cause":
        case "9 Deceduto per altre cause, specificare":
            clinicalStatus = "DEAD FOR OTHER REASON";
            break;
        case "Deceduto per secondo tumore":
            clinicalStatus = "DEAD FOR SECOND TUMOUR";
            break;
        case "Deceduto per cause non note":
            clinicalStatus = "DEAD FOR UNKNOWN CAUSES";
            break;
        default:
            clinicalStatus = null;
    }
    switch (patient['D_RISP_MAX']) {
        case "RP (Risposta parziale >50%)":
            maxRes = "PARTIAL RESPONSE >50% (RP)";
            break;
        case "MO (Risposta parziale 25-50%)":
            maxRes = "PARTIAL RESPONSE 25-50% (MO)";
            break;
        case "PM (Progressione di malattia)":
            maxRes = "DISEASE PROGRESSION (PM)";
            break;
        case "RC (Risposta completa)":
            maxRes = "COMPLETE RESPONSE (RC)";
            break;
        case "ST (Non risposta)":
            maxRes = "NO RESPONSE (ST)";
            break;
        case "VGPR (Risposta parziale 90-99%)":
            maxRes = "PARTIAL RESPONSE 90-99% (VGPR)";
            break;
        case "PDV (Perso di vista)":
            maxRes = "LOST SIGHT";
            break;
        case "Non valutabile":
            maxRes = "NOT EVALUABLE";
            break;
        case "Non so":
            maxRes = "N.D.";
            break;
        default:
            maxRes = null;
    }
    switch (patient['GDE_D_STATO_MYCN']) {
        case "Amplificazione presente":
            mycn = "AMPL";
            break;
        case "Amplificazione assente":
        case "Amplificazione e gain assenti":
            mycn = "NO AMPL";
            break;
        case "MYCN gain":
            mycn = "MYCN GAIN";
            break;
        case "Amplificazione focale":
            mycn = "FOCAL AMPL";
            break;
        case "Risultato dubbio o non interpretabile":
        case "Non so":
            mycn = "N.D.";
            break;
        case "Non eseguita":
            mycn = "NOT EXECUTED";
            break;
        default:
            mycn = null;
    }
    switch (patient['D_TIPO_RECIDIVAPM']) {
        case "Locale":
            relapse = "LOCAL";
            break;
        case "Metastatica":
            relapse = "METASTATIC";
            break;
        case "Combinata":
            relapse = "COMBINED";
            break;
        case "Non so":
            relapse = "N.D.";
            break;
        default:
            relapse = null;
    }
    switch (patient['GDE_D_ISTOTIPO']) {
        case "GN maturo":
            hysto = "GN MATURE";
            break;
        case "GN in maturazione":
            hysto = "GN MATURING";
            break;
        case "GN NAS":
            hysto = "GN N.O.S";
            break;
        case "GNB intermixed":
            hysto = "GNB INTERMIXED";
            break;
        case "GNB NAS":
            hysto = "GNB N.O.S.";
            break;
        case "GNB nodulare con nodulo(i) differenziante(i)":
            hysto = "GNB NODULAR - DIFFERENTIATING";
            break;
        case "GNB nodulare con nodulo(i) scarsamente differenziato(i)":
            hysto = "GNB NODULAR - POORLY DIFFERENTIATED";
            break;
        case "nb indifferenziato":
            hysto = "GNB - UNDIFFERENTIATED";
            break;
        case "NB differenziante":
            hysto = "NB DIFFERENTIATING";
            break;
        case "NB NAS":
            hysto = "NB N.O.S.";
            break;
        case "NB scarsamente differenziato":
            hysto = "NB POORLY DIFFERENTIATED";
            break;
        case "NB indifferenziato":
            hysto = "NB UNDIFFERENTIATED";
            break;
        case "No tumore neuroblastico":
            hysto = "NO NEUROBLASTIC TUMOUR";
            break;
        case "Tumore neuroblastico non classificabile":
            hysto = "NOT CLASSIFIABLE";
            break;
        case "Paziente non operato alla diagnosi":
            hysto = "PATIENT NEVER UNDER SURGERY";
            break;
        default:
            hysto = null;
    }
    switch (patient['D_SEDETUM']) {
        case "Addome gangli retroperitoneali":
            primit = "RETROPERITONEAL GANGLIA";
            break;
        case "Addome surrene":
            primit = "ABDOMEN SUPRARENAL GLANDS";
            break;
        case "Addome NAS":
            primit = "ABDOMEN N.O.S.";
            break;
        case "Torace":
            primit = "THORAX";
            break;
        case "Toraco-addominale":
            primit = "THORACO-ABDOMINAL";
            break;
        case "Pelvi":
            primit = "PELVIS";
            break;
        case "Collo":
            primit = "NECK";
            break;
        case "Multiple non contigue":
            primit = "MULTIPLE NOT CONTIGUOUS";
            break;
        case "Non identificata":
            primit = "NOT IDENTIFIED";
            break;
        case "Altra":
            primit = "OTHER";
            break;
        case "Non so":
            primit = "N.D.";
            break;
        default:
            primit = null;
    }

    switch (patient['D_NOME_PROT']) {
        case undefined:
        case null:
            protocol = null;
            break;
        case "Non so":
            protocol = "N.D.";
            break;
        case "LNESG1":
            protocol = "LNESG 1";
            break;
        case "LNESG2":
            protocol = "LNESG 2";
            break;
        default:
            protocol = patient['D_NOME_PROT'] !== "" ? patient['D_NOME_PROT'].toUpperCase() : null;
    }

    // logger.log('info',". dgnDate " + dgnDate._i + ". relDate " + relDate._i + ". FuDate " + FuDate._i+ ". daySurvOver " + daySurvOver+ ". daySurvProg " + daySurvProg);
    let metadata = {
        "inss": {
            "value": inss,
            "group": "Clinical Details"
        },
        "inrgss": {
            "value": inrg,
            "group": "Clinical Details"
        },
        "maximum_response": {
            "value": maxRes,
            "group": "Clinical Details"
        },
        "ploidy": {
            "value": ploidy,
            "group": "Biological Details"
        },
        // if data relapse_date
        "relapse": {
            "value": rel,
            "group": "Clinical Details"
        },
        "histology": {
            "value": hysto,
            "group": "Clinical Details"
        },
        "mycn_status": {
            "value": mycn,
            "group": "Biological Details"
        },
        "primary_site": {
            "value": primit,
            "group": "Clinical Details"
        },
        "relapse_date": {
            "value": relDate !== null ? relDate.format('YYYY-MM-DD') : null,
            "group": "Clinical Details"
        },
        "relapse_type": {
            "value": relapse,
            "group": "Clinical Details"
        },
        "diagnosis_age": {
            "value": patient['ETADG'],
            "unit": "month",
            "group": "Clinical Details"
        },
        "diagnosis_date": {
            "value": dgnDate.format('YYYY-MM-DD'),
            "group": "Clinical Details"
        },
        "clinical_protocol": {
            "value": protocol,
            "group": "Clinical Details"
        },
        "last_follow_up_date": {
            "value": FuDate ? FuDate.format('YYYY-MM-DD') : null,
            "group": "Clinical Details"
        },
        "italian_nb_registry_id": {
            "value": patient['Registry ID'],
            "group": "Clinical Details"
        },
        "clinical_follow_up_status": {
            "value": clinicalStatus,
            "group": "Clinical Details"
        },
        "survival_progfree": {
            "unit": "day",
            "value": daySurvProg,
            "group": "Events & Survival Info"
        },
        "survival_overall": {
            "unit": "day",
            "value": daySurvOver,
            "group": "Events & Survival Info"
        },
        "event_progfree": {
            "value": evProg,
            "group": "Events & Survival Info"
        },
        "event_overall": {
            "value": evOver,
            "group": "Events & Survival Info"
        }
    };

    // console.log(metadata);
    return metadata;
}

/**
 * @method
 * @name composeVCFMetadata
 * @description compose a vcf metadata object
 * @param{object} - datum: vcf information
 * @return{object} - object with metadata structure
 *
 */

function composeVCFMetadata (datum, machine, capture) {
    let chr;
    let pos;
    let id;
    let qual;
    let ref;
    let alt;
    let filter;
    let gt;
    let gtAllele;
    let pl;
    let gq;
    let dp;
    let ad;
    let sangerValid;
    let pathological;
    let varsomeLink;
    let metadata = [];
    chr = datum.chr && datum.chr.split("chr").length > 1 ? datum.chr : "chr" + datum.chr;
    pos = datum.pos && parseInt(datum.pos);
    id = datum.id && (datum.id !== "." || datum.id !== "0") ? datum.id : null;
    qual = datum.qual ? parseFloat(datum.qual) : null;
    ref = datum.ref && datum.ref;
    alt = datum.alt && datum.alt.split(",");
    filter = datum.filter ? datum.filter : null;
    pl = datum.PL ? datum.PL : datum.sampleinfo[0] && datum.sampleinfo[0].PL ? datum.sampleinfo[0].PL : datum.varinfo.PL ? datum.varinfo.PL : null;
    gq = datum.GQ ? parseInt(datum.GQ) : datum.sampleinfo[0] && datum.sampleinfo[0].GQ ? parseInt(datum.sampleinfo[0].GQ) : datum.varinfo.GQ ? parseInt(datum.varinfo.GQ) : null;
    gt = datum.GT ? datum.GT : datum.sampleinfo[0] && datum.sampleinfo[0].GT ? datum.sampleinfo[0].GT : datum.varinfo.GT ? datum.varinfo.GT : null;
    dp = datum.DP ? parseFloat(datum.DP) : datum.sampleinfo[0] && datum.sampleinfo[0].DP ? parseFloat(datum.sampleinfo[0].DP) : datum.varinfo.DP ? parseFloat(datum.varinfo.DP) : null;
    ad = datum.AD ? parseInt(datum.AD) : datum.sampleinfo[0] && datum.sampleinfo[0].AD ? parseInt(datum.sampleinfo[0].AD) : datum.varinfo.AD ? parseInt(datum.varinfo.AD) : datum.CLCAD2 ? parseInt(datum.CLCAD2) : datum.sampleinfo[0] && datum.sampleinfo[0].CLCAD2 ? parseInt(datum.sampleinfo[0].CLCAD2) : datum.varinfo.CLCAD2 ? parseInt(datum.varinfo.CLCAD2) : null;

    switch (gt) {
        case undefined:
        case null:
            gtAllele = null;
            break;
        case "0/0":
        case "0|0":
            gtAllele = 0;
            break;
        case "0/1":
        case "1/0":
        case "0|1":
        case "1|0":
            gtAllele = 1;
            break;
        case "1/1":
        case "1|1":
            gtAllele = 2;
            break;
        default:
            gtAllele = null;
            gt = "./.";
    }

    alt.forEach(al => {
        varsomeLink = "https://varsome.com/variant/hg19/" + chr + "-" + pos + "-" + ref + "-" + alt;

        let metadatum = {
            "chr": {
                "value": chr,
                "group": "Variant Details"
            },
            "pos": {
                "value": pos,
                "group": "Variant Details"
            },
            "id": {
                "value": id,
                "group": "Variant Details"
            },
            "qual": {
                "value": qual,
                "group": "Variant Details"
            },
            // if data relapse_date
            "ref": {
                "value": ref,
                "group": "Variant Details"
            },
            "alt": {
                "value": al,
                "group": "Variant Details"
            },
            "filter": {
                "value": filter,
                "group": "Variant Details"
            },
            "gt": {
                "value": gt,
                "group": "Genotype Details"
            },
            "gt_allele": {
                "value": gtAllele,
                "group": "Genotype Details"
            },
            "pl": {
                "value": pl,
                "group": "Genotype Details"
            },
            "gq": {
                "value": gq,
                "group": "Genotype Details"
            },
            "dp": {
                "value": dp,
                "group": "Genotype Details"
            },
            "ad": {
                "value": ad,
                "group": "Genotype Details"
            },
            "sanger_validation": {
                "value": "N.D.",
                "group": "Genotype Details"
            },
            "pathological": {
                "value": false,
                "group": "Genotype Details"
            },
            //  "machine": {
            //      "value": machine,
            //      "group": "Read Details"
            //  },
            //  "capture": {
            //      "value": capture,
            //      "group": "Read Details"
            //  },
            "varsome_link": {
                "value": varsomeLink,
                "group": "Links"
            }
        };
        metadata.push(metadatum);
    });

    return metadata;
}

function composeVCFNGSMetadata (datum, machine, capture) {
    let chr;
    let pos;
    let id;
    let qual;
    let ref;
    let alt;
    let filter;
    let an;
    let af;
    let ac;
    let aq;
    let cases_all;
    let cc_all_all;
    let cc_dom_all;
    let cc_geno_all;
    let cc_rec_all;
    let cc_trend_all;
    let controls_all;
    let _var;
    let varinfo;
    chr = datum.chr && datum.chr.split("chr").length > 1 ? datum.chr : "chr" + datum.chr;
    pos = datum.pos && parseInt(datum.pos);
    id = datum.id && (datum.id !== "." || datum.id !== "0") ? datum.id : null;
    qual = datum.qual ? parseFloat(datum.qual) : null;
    ref = datum.ref && datum.ref;
    alt = datum.alt && datum.alt;
    filter = datum.filter ? datum.filter : null;
    af = datum.varinfo.AF ? parseFloat(datum.varinfo.AF) : null;
    an = datum.varinfo.AN ? parseFloat(datum.varinfo.AN) : null;
    ac = datum.varinfo.AC ? parseFloat(datum.varinfo.AC) : null;
    aq = datum.varinfo.AQ ? parseFloat(datum.varinfo.AQ) : null;
    cases_all = datum.varinfo.Cases_ALL ? datum.varinfo.Cases_ALL : null;
    cc_all_all = datum.varinfo.CC_ALL_ALL ? datum.varinfo.CC_ALL_ALL : null;
    cc_dom_all = datum.varinfo.CC_DOM_ALL ? datum.varinfo.CC_DOM_ALL : null;
    cc_geno_all = datum.varinfo.CC_GENO_ALL ? datum.varinfo.CC_GENO_ALL : null;
    cc_rec_all = datum.varinfo.CC_REC_ALL ? datum.varinfo.CC_REC_ALL : null;
    cc_trend_all = datum.varinfo.CC_TREND_ALL ? datum.varinfo.CC_TREND_ALL : null;
    controls_all = datum.varinfo.Controls_ALL ? datum.varinfo.Controls_ALL : null;
    _var = datum.varinfo.VAR ? datum.varinfo.VAR : null;
    varinfo = datum.varinfo.VARINFO ? datum.varinfo.VARINFO : null;

    let metadata = { };

    if (chr) {
        metadata.chr = {
            "value": chr,
            "group": "Variant Details"
        };
    }
    if (pos) {
        metadata.pos = {
            "value": pos,
            "group": "Variant Details"
        };
    }
    if (id) {
        metadata.id = {
            "value": id,
            "group": "Variant Details"
        };
    }
    if (qual) {
        metadata.qual = {
            "value": qual,
            "group": "Variant Details"
        };
    }
    if (ref) {
        metadata.ref = {
            "value": ref,
            "group": "Variant Details"
        };
    }
    if (alt) {
        metadata.alt = {
            "value": alt,
            "group": "Variant Details"
        };
    }
    if (filter) {
        metadata.filter = {
            "value": filter,
            "group": "Variant Details"
        };
    }
    if (af) {
        metadata.af = {
            "value": af,
            "group": "Annotations"
        };
    }
    if (an) {
        metadata.an = {
            "value": an,
            "group": "Annotations"
        };
    }
    if (ac) {
        metadata.ac = {
            "value": ac,
            "group": "Annotations"
        };
    }
    if (aq) {
        metadata.aq = {
            "value": aq,
            "group": "Annotations"
        };
    }
    if (cases_all) {
        metadata.cases_all = {
            "value": cases_all,
            "group": "Annotations"
        };
    }
    if (cc_all_all) {
        metadata.cc_all_all = {
            "value": cc_all_all,
            "group": "Annotations"
        };
    }
    if (cc_dom_all) {
        metadata.cc_dom_all = {
            "value": cc_dom_all,
            "group": "Annotations"
        };
    }
    if (cc_all_all) {
        metadata.cc_all_all = {
            "value": cc_all_all,
            "group": "Annotations"
        };
    }

    if (cc_geno_all) {
        metadata.cc_geno_all = {
            "value": cc_geno_all,
            "group": "Annotations"
        };
    }
    if (cc_rec_all) {
        metadata.cc_rec_all = {
            "value": cc_rec_all,
            "group": "Annotations"
        };
    }
    if (cc_trend_all) {
        metadata.cc_trend_all = {
            "value": cc_trend_all,
            "group": "Annotations"
        };
    }
    if (controls_all) {
        metadata.controls_all = {
            "value": controls_all,
            "group": "Annotations"
        };
    }
    if (_var) {
        metadata.var = {
            "value": _var,
            "group": "Annotations"
        };
    }
    if (varinfo) {
        metadata.varinfo = {
            "value": varinfo,
            "group": "Annotations"
        };
    }

    return metadata;
}

function composeVCFNGSPersonalInfoMetadata (datum) {
    let ad;
    let dp;
    let gq;
    let gt;
    let pl;

    ad = datum.AD ? datum.AD : null;
    dp = datum.DP ? parseFloat(datum.DP) : null;
    gq = datum.GQ ? parseFloat(datum.GQ) : null;
    gt = datum.GT ? datum.GT : null;
    pl = datum.PL ? datum.PL : null;

    let metadata = { };

    if (ad) {
        metadata.ad = {
            "value": ad,
            "group": "Variant Details"
        };
    }
    if (dp) {
        metadata.dp = {
            "value": dp,
            "group": "Variant Details"
        };
    }
    if (gq) {
        metadata.gq = {
            "value": gq,
            "group": "Variant Details"
        };
    }
    if (gt) {
        metadata.gt = {
            "value": gt,
            "group": "Variant Details"
        };
    }
    if (pl) {
        metadata.pl = {
            "value": pl,
            "group": "Variant Details"
        };
    }

    return metadata;
}

function composeNSGAnalysisMetadata (analyses) {
    let metadata = {
        "target": {
            "group": "Details",
            "value": analyses[0]['target'] ? analyses[0]['target'].toUpperCase() === 'WGS' ? 'WHOLE GENOME' : analyses[0]['target'].toUpperCase() === 'WES' ? "EXOME" : analyses[0]['target'].toUpperCase() === 'PANEL' ? "PANEL" : null : null
        },
        "platform": {
            "group": "Details",
            "value": analyses[0]['platform']
        },
        "target_details": {
            "group": "Details",
            "value": analyses[0]['target_details']
        },
        "sequencing_date": {
            "group": "FASTQ File Info",
            "value": analyses[0]['sequencing_date']
        },
        "fastq_file_path_r1": {
            "loop": "Sequencing Units",
            "group": "FASTQ File Info",
            "values": []
        },
        "fastq_file_path_r2": {
            "loop": "Sequencing Units",
            "group": "FASTQ File Info",
            "values": []
        },
        "lane": {
            "loop": "Sequencing Units",
            "group": "FASTQ File Info",
            "values": []
        },
        "flow_cell": {
            "loop": "Sequencing Units",
            "group": "FASTQ File Info",
            "values": []
        }
    };
    let R1Array = [];
    let R2Array = [];
    let laneArray = [];
    let flowCellArray = [];
    analyses.map(an => {
        // let template = {};
        // da pescare modello della analisi

        if (an['fq1']) {
            R1Array.push(an['fq1']);
        }
        if (an['fq2']) {
            R2Array.push(an['fq2']);
        }
        if (an['lane']) {
            laneArray.push(an['lane']);
        }
        if (an['flow_cell']) {
            flowCellArray.push(an['flow_cell']);
        }
    });

    metadata.fastq_file_path_r1.values = R1Array;
    metadata.fastq_file_path_r2.values = R2Array;
    metadata.lane.values = laneArray;
    metadata.flow_cell.values = flowCellArray;

    return metadata;
}

function * RewritePathNGS (analyses, daemon) {
    // debugger;
    logger.log('info', 'RewritePathNGS');

    // let R1Array = [], R2Array = [], laneArray = [], flowCellArray = [];
    yield BluebirdPromise.each(analyses, co.wrap(function * (an) {
        // logger.log('info', 'RewritePathNGS: ' + an['fq1']);

        let oldPathSplittedFq1 = an['fq1'].split('/');
        let filenameFq1 = oldPathSplittedFq1[oldPathSplittedFq1.length - 1];
        // logger.log('info', 'RewritePathNGS - filenameFq1: ' + filenameFq1);

        let newPathFq1 = process.env.NGSPrefixDebugPath + config.NGSFilesBasePath + '/' + analyses[0].subjectCode + '/' + analyses[0].biobankCode + '/' + an.flow_cell + '.' + an.lane;

        // logger.log('info', 'RewritePathNGS - newPathFq1: ' + newPathFq1);

        // let template = {};
        if (!fs.existsSync(newPathFq1)) {
            try {
                fs.mkdirSync(newPathFq1, { recursive: true });
                logger.log('info', 'RewritePathNGS -  newPathFq1 created: ' + newPathFq1);
            } catch (error) {
                let errorString = error.message;
                logger.log('info', errorString);
                daemon = yield DaemonService.ErrorDeamon(daemon, errorString, config.bearerToken);
                return;
            }
        } else {
            logger.log('info', 'RewritePathNGS -  newPathFq1 exist: skipped');
        }

        let newFilePath1 = newPathFq1 + '/' + filenameFq1;
        // logger.log('info', 'RewritePathNGS -  file path: ' + newFilePath1);

        // da pescare modello della analisi
        if (!fs.existsSync(newFilePath1)) {
            try {
                fs.renameSync(process.env.NGSPrefixDebugPath + an['fq1'], newFilePath1);
                logger.log('info', 'RewritePathNGS -  file moved');
            } catch (error) {
                let errorString = error.message;
                logger.log('info', errorString);
                daemon = yield DaemonService.ErrorDeamon(daemon, errorString, config.bearerToken);
                return;
            }
        } else {
            logger.log('info', 'RewritePathNGS -  file exist - skipped');
        }

        an['fq1'] = newFilePath1.replace(process.env.NGSPrefixDebugPath, '');

        // logger.log('info', 'RewritePathNGS -  final path: ' + an['fq1']);

        // logger.log('info', 'RewritePathNGS -  -----------------------------------');

        // logger.log('info', 'RewritePathNGS: ' + an['fq2']);

        let oldPathSplittedFq2 = an['fq2'].split('/');
        let filenameFq2 = oldPathSplittedFq2[oldPathSplittedFq2.length - 1];
        // logger.log('info', 'RewritePathNGS - filenameFq1: ' + filenameFq2);

        let newPathFq2 = process.env.NGSPrefixDebugPath + config.NGSFilesBasePath + '/' + analyses[0].subjectCode + '/' + analyses[0].biobankCode + '/' + an.flow_cell + '.' + an.lane;

        // logger.log('info', 'RewritePathNGS - newPathFq1: ' + newPathFq2);

        // let template = {};
        if (!fs.existsSync(newPathFq2)) {
            try {
                fs.mkdirSync(newPathFq2, { recursive: true });
                logger.log('info', 'RewritePathNGS -  newPathFq2 created');
            } catch (error) {
                let errorString = error.message;
                logger.log('info', errorString);
                daemon = yield DaemonService.ErrorDeamon(daemon, errorString, config.bearerToken);
                return;
            }
        } else {
            logger.log('info', 'RewritePathNGS -  newPathFq2 exist: skipped');
        }

        let newFilePath2 = newPathFq2 + '/' + filenameFq2;
        // logger.log('info', 'RewritePathNGS -  file path: ' + newFilePath2);

        if (!fs.existsSync(newFilePath2)) {
            try {
                fs.renameSync(process.env.NGSPrefixDebugPath + an['fq2'], newFilePath2);
                logger.log('info', 'RewritePathNGS -  file moved');
            } catch (error) {
                let errorString = error.message;
                logger.log('info', errorString);
                daemon = yield DaemonService.ErrorDeamon(daemon, errorString, config.bearerToken);
                return;
            }
        } else {
            logger.log('info', 'RewritePathNGS -  file already moved - skipped');
        }

        an['fq2'] = newFilePath2.replace(process.env.NGSPrefixDebugPath, '');
        // logger.log('info', 'RewritePathNGS -  final path: ' + an['fq1']);

        // logger.log('info', 'RewritePathNGS -  -----------------------------------');
    }));

    return {
        "analyses": analyses,
        "daemon": daemon
    };
}

/**
 * @method
 * @name composeVCFMetadataAnnotation
 * @description compose a vcf metadata object
 * @param{object} - datum: vcf information
 * @param{object} - freqInfo: frequency information
 * @param{object} - geneInfo: gene information
 * @return{object} - object with the coplmete metadata structure
 *
 */

function composeVCFMetadataAnnotation (datum, freqInfo, geneInfo) {
    if (freqInfo) {
        //  _.forEach(freqInfo, (info, name)=>{
        if (freqInfo.af) {
            var af = scientificToDecimal(freqInfo.af);
            if (isNaN(parseFloat(af)) || isNaN(parseFloat(freqInfo.af))) {
                console.log(af, freqInfo.af);
            }
            datum.metadata['af'] = {
                group: 'Frequency Annotation',
                value: freqInfo.af
            };
        }
        if (freqInfo.id_pol !== ".") {
            if (datum.metadata['id'].value === "." || (datum.metadata['id'].value !== "." && datum.metadata['id'].value !== freqInfo.id_pol)) {
                datum.metadata['id'] = {
                    group: 'Variant Details',
                    value: freqInfo.id_pol
                };
            }
        }
        if (freqInfo.an) {
            datum.metadata['an'] = {
                group: 'Frequency Annotation',
                value: freqInfo.an
            };
        }
        if (freqInfo.ac) {
            datum.metadata['ac'] = {
                group: 'Frequency Annotation',
                value: freqInfo.ac
            };
        }
        if (freqInfo.sift) {
            datum.metadata['sift'] = {
                group: 'Frequency Annotation',
                value: freqInfo.sift
            };
        }

        if (freqInfo.polyphen) {
            datum.metadata['polyphen'] = {
                group: 'Frequency Annotation',
                value: freqInfo.polyphen
            };
        }
        if (freqInfo.clinvar_meas) {
            datum.metadata['clinvar_measureset_id'] = {
                group: 'Frequency Annotation',
                value: freqInfo.clinvar_meas
            };
        }
        if (freqInfo.clinvar_path) {
            datum.metadata['clinvar_pathogenic'] = {
                group: 'Frequency Annotation',
                value: freqInfo.clinvar_path
            };
        }
        if (freqInfo.clinvar_confl) {
            datum.metadata['clinvar_conflicted'] = {
                group: 'Frequency Annotation',
                value: freqInfo.clinvar_confl
            };
        }
        if (freqInfo.clinvar_mut) {
            datum.metadata['clinvar_mut'] = {
                group: 'Frequency Annotation',
                value: freqInfo.clinvar_mut
            };
        }
    }

    if (geneInfo) {
        datum.metadata['gene_name'] = {
            group: 'Gene Annotation',
            value: geneInfo.name
        };
    }

    return datum;
}

function composeBioAnMetadata (datum) {
    let sample = {};
    let bioAn = {};

    if (datum.HVA) {
        bioAn['hva'] = {
            group: 'Analysis Results',
            value: datum.HVA,
            unit: datum.HVA_UNIT
        };
    }
    if (datum.VMA) {
        bioAn['vma'] = {
            group: 'Analysis Results',
            value: datum.VMA,
            unit: datum.VMA_UNIT
        };
    }
    if (datum['3-MT']) {
        bioAn['_3_mt'] = {
            group: 'Analysis Results',
            value: datum['3-MT'],
            unit: datum['3-MT_UNIT']
        };
    }
    if (datum.M) {
        bioAn['m'] = {
            group: 'Analysis Results',
            value: datum.M,
            unit: datum.M_UNIT
        };
    }
    if (datum.NM) {
        bioAn['nm'] = {
            group: 'Analysis Results',
            value: datum.NM,
            unit: datum.NM_UNIT
        };
    }
    if (datum.E) {
        bioAn['e'] = {
            group: 'Analysis Results',
            value: datum.E,
            unit: datum.E_UNIT
        };
    }
    if (datum.NE) {
        bioAn['ne'] = {
            group: 'Analysis Results',
            value: datum.NE,
            unit: datum.NE_UNIT
        };
    }
    if (datum.D) {
        bioAn['d'] = {
            group: 'Analysis Results',
            value: datum.D,
            unit: datum.D_UNIT
        };
    }
    if (datum.Crea) {
        bioAn['crea'] = {
            group: 'Analysis Results',
            value: datum.Crea,
            unit: datum.Crea_UNIT
        };
    }
    // SAMPLE
    if (!datum['PLASMA']) {
        if (datum['Sampling Date']) {
            sample['sampling_date'] = {
                group: 'Fluid Info',
                value: datum['Sampling Date'] ? moment(datum['Sampling Date'], "DD-MM-YYYY").format("YYYY-MM-DD") : null
            };
        }

        sample['pathology'] = {
            group: 'Fluid Info',
            value: 'NEUROBLASTOMA'
        };

        sample['sample_codification'] = {
            group: 'Fluid Info',
            value: 'URINE'
        };

        if (datum.Quantity) {
            sample['quantity'] = {
                group: 'Analysis Results',
                value: datum.Quantity,
                unit: 'ml'
            };
        }

        if (datum.City) {
            sample['city'] = {
                group: 'Analysis Results',
                value: datum.City
            };
        }
        if (datum.Hospital) {
            sample['hospital'] = {
                group: 'Analysis Results',
                value: datum.Hospital
            };
        }
        if (datum.Unit) {
            sample['unit'] = {
                group: 'Analysis Results',
                value: datum.Unit
            };
        }
        if (datum['Tumour Status']) {
            sample['tumour_status'] = {
                group: 'Analysis Results',
                value: datum['Tumour Status']
            };
        }
    }

    return {
        sample: sample,
        bioAn: bioAn
    };
}

function composeNKCellsMetadata (datum) {
    let nkcell = {};

    if (datum.pd1) {
        nkcell.pd1 = {
            "unit": "%",
            "group": "Immune Checkpoint Receptors",
            "value": datum.pd1
        };
    }
    if (datum.lag_3) {
        nkcell.lag_3 = {
            "unit": "%",
            "group": "Immune Checkpoint Receptors",
            "value": datum.lag_3
        };
    }
    if (datum.tigit) {
        nkcell.tigit = {
            "unit": "%",
            "group": "Immune Checkpoint Receptors",
            "value": datum.tigit
        };
    }
    if (datum.tim_3) {
        nkcell.tim_3 = {
            "unit": "%",
            "group": "Immune Checkpoint Receptors",
            "value": datum.tim_3
        };
    }
    if (datum.mean_cd2) {
        nkcell.mean_cd2 = {
            "group": "Activating Receptors",
            "value": datum.mean_cd2
        };
    }
    if (datum.mean_cd69) {
        nkcell.mean_cd69 = {
            "group": "Activating Receptors",
            "value": datum.mean_cd69
        };
    }
    if (datum.mean_cxcr1) {
        nkcell.mean_cxcr1 = {
            "group": "Chemokine Receptors",
            "value": datum.mean_cxcr1
        };
    }
    if (datum.mean_cxcr4) {
        nkcell.mean_cxcr4 = {
            "group": "Chemokine Receptors",
            "value": datum.mean_cxcr4
        };
    }
    if (datum.mean_nkg2d) {
        nkcell.mean_nkg2d = {
            "group": "Activating Receptors",
            "value": datum.mean_nkg2d
        };
    }
    if (datum.mean_nkp30) {
        nkcell.mean_nkp30 = {
            "group": "Activating Receptors",
            "value": datum.mean_nkp30
        };
    }
    if (datum.mean_nkp46) {
        nkcell.mean_nkp46 = {
            "group": "Activating Receptors",
            "value": datum.mean_nkp46
        };
    }
    if (datum.mean_cx3cr1) {
        nkcell.mean_cx3cr1 = {
            "group": "Chemokine Receptors",
            "value": datum.mean_cx3cr1
        };
    }
    if (datum.mean_dnam_1) {
        nkcell.mean_dnam_1 = {
            "group": "Activating Receptors",
            "value": datum.mean_dnam_1
        };
    }
    if (datum.percentage_cd2) {
        nkcell.percentage_cd2 = {
            "unit": "%",
            "group": "Activating Receptors",
            "value": datum.percentage_cd2
        };
    }
    if (datum.percentage_ccr7) {
        nkcell.percentage_ccr7 = {
            "unit": "%",
            "group": "Chemokine Receptors",
            "value": datum.percentage_ccr7
        };
    }
    if (datum.percentage_cd69) {
        nkcell.percentage_cd69 = {
            "unit": "%",
            "group": "Activating Receptors",
            "value": datum.percentage_cd69
        };
    }
    if (datum.percentage_cxcr1) {
        nkcell.percentage_cxcr1 = {
            "unit": "%",
            "group": "Chemokine Receptors",
            "value": datum.percentage_cxcr1
        };
    }
    if (datum.percentage_cxcr4) {
        nkcell.percentage_cxcr4 = {
            "unit": "%",
            "group": "Chemokine Receptors",
            "value": datum.percentage_cxcr4
        };
    }
    if (datum.percentage_dnam1) {
        nkcell.percentage_dnam1 = {
            "unit": "%",
            "group": "Activating Receptors",
            "value": datum.percentage_dnam1
        };
    }
    if (datum.percentage_nkg2d) {
        nkcell.percentage_nkg2d = {
            "unit": "%",
            "group": "Activating Receptors",
            "value": datum.percentage_nkg2d
        };
    }
    if (datum.percentage_nkp30) {
        nkcell.percentage_nkp30 = {
            "unit": "%",
            "group": "Activating Receptors",
            "value": datum.percentage_nkp30
        };
    }
    if (datum.percentage_nkp46) {
        nkcell.percentage_nkp46 = {
            "unit": "%",
            "group": "Activating Receptors",
            "value": datum.percentage_nkp46
        };
    }
    if (datum.percentage_cx3cr1) {
        nkcell.percentage_cx3cr1 = {
            "unit": "%",
            "group": "Chemokine Receptors",
            "value": datum.percentage_cx3cr1
        };
    }
    if (datum.mem154_positive_nk_cells) {
        nkcell.mem154_positive_nk_cells = {
            "unit": "%",
            "group": "CD16",
            "value": datum.mem154_positive_nk_cells
        };
    }
    if (datum.percentage_negative_cd16) {
        nkcell.percentage_negative_cd16 = {
            "unit": "%",
            "group": "CD16",
            "value": datum.percentage_negative_cd16
        };
    }
    if (datum.percentage_negative_cd56) {
        nkcell.percentage_negative_cd56 = {
            "unit": "%",
            "group": "CD56",
            "value": datum.percentage_negative_cd56
        };
    }
    if (datum.percentage_positive_cd16) {
        nkcell.percentage_positive_cd16 = {
            "unit": "%",
            "group": "CD16",
            "value": datum.percentage_positive_cd16
        };
    }
    if (datum.percentage_positive_cd56) {
        nkcell.percentage_positive_cd56 = {
            "unit": "%",
            "group": "CD56",
            "value": datum.percentage_positive_cd56
        };
    }
    if (datum.totat_percentage_nk_cells) {
        nkcell.totat_percentage_nk_cells = {
            "unit": "%",
            "group": "CD56",
            "value": datum.totat_percentage_nk_cells
        };
    }
    if (datum.percentage_cd107a_by_cd16_) {
        nkcell.percentage_cd107a_by_cd16_ = {
            "unit": "%",
            "group": "Degranulation",
            "value": datum.percentage_cd107a_by_cd16_
        };
    }
    if (datum.percentage_adaptive_nk_cells) {
        nkcell.percentage_adaptive_nk_cells = {
            "unit": "%",
            "group": "Adaptive NK Cells",
            "value": datum.percentage_adaptive_nk_cells
        };
    }

    return nkcell;
}

function scientificToDecimal (num) {
    // if the number is in scientific notation remove it
    // eslint-disable-next-line no-useless-escape
    if (/\d+\.?\d*e[\+\-]*\d+/i.test(num)) {
        var zero = '0';
        var parts = String(num).toLowerCase().split('e'); // split into coeff and exponent
        var e = parts.pop();
        // store the exponential part
        var l = Math.abs(e); // get the number of zeros
        var sign = e / l;
        var coeffArray = parts[0].split('.');
        if (sign === -1) {
            num = zero + '.' + new Array(l).join(zero) + coeffArray.join('');
        } else {
            var dec = coeffArray[1];
            if (dec) l = l - dec.length;
            num = coeffArray.join('') + new Array(l + 1).join(zero);
        }
    }

    return parseFloat(num);
}

const re1Vcf = '((?:[a-zA-Z0-9-]+))'; // owner-project    //
const re2Vcf = '(-)'; // indent   // Patient-Code
const re3Vcf = '((?:[a-zA-Z0-9-]+))'; // code     //
// const re4Vcf = '(_)';            // underscore 1
//  const re5Vcf='.*?';                              // family id
const re6Vcf = '(_)'; // underscore 2
const re7Vcf = '((?:[a-z][a-z]+))'; // sample type   //
const re8Vcf = '(-)'; // sample indent // Sample
const re9Vcf = '((?:[a-zA-Z0-9-]+))'; // sample number //
const re10Vcf = '(_)'; // underscore 3
const re11Vcf = '(ILL|ION)'; // Machine: ION or ILL
const re12Vcf = '(_)'; // underscore 4
const re13Vcf = '.*?'; // Capture
const re14Vcf = '(\\.)'; // dot
const re15Vcf = '(vcf)'; // vcf extension
const regexExpValidation = {
    'vcf': new RegExp(re1Vcf + re2Vcf + re3Vcf + re6Vcf + re7Vcf + re8Vcf + re9Vcf + re10Vcf + re11Vcf + re12Vcf + re13Vcf + re14Vcf + re15Vcf, ["i"])
};

module.exports.parseFileName = parseFileName;
module.exports.composeCNBInfoMetadata = composeCNBInfoMetadata;
module.exports.composeVCFMetadata = composeVCFMetadata;
module.exports.composeVCFNGSMetadata = composeVCFNGSMetadata;
module.exports.composeVCFNGSPersonalInfoMetadata = composeVCFNGSPersonalInfoMetadata;
module.exports.scientificToDecimal = scientificToDecimal;
module.exports.composeVCFMetadataAnnotation = composeVCFMetadataAnnotation;
module.exports.composeCGHProcessedMetadata = composeCGHProcessedMetadata;
module.exports.composeCNVMetadata = composeCNVMetadata;
module.exports.getFilesInFolder = getFilesInFolder;
module.exports.composeCGHMetadata = composeCGHMetadata;
module.exports.composeBioAnMetadata = composeBioAnMetadata;
module.exports.composeNKCellsMetadata = composeNKCellsMetadata;
module.exports.composeNSGAnalysisMetadata = composeNSGAnalysisMetadata;
module.exports.RewritePathNGS = RewritePathNGS;
