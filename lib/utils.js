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
 let enumFields = ['Genomic Profile'];
 let moment = require('moment-timezone');

/**
 * @method
 * @name getFilesInFolder
 * @param{string} folder - the folder where you search for files
 * @param{string} ext - extension to be matched
 * @param{boolean} isDeep - if true search subfolders as well
 * @return{Array} files - the list of files in the directory
 */
 function getFilesInFolder(folder, ext, isDeep) {
     let res = [];
     fs.readdirSync(folder).forEach(function(file) {

         file = folder + "/" + file;
         let stat = fs.statSync(file);

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
     let res = {};
     _.each(cghMetadataArr, function(metadatumArr) {
         if (metadatumArr[0] === 'Window Size') {
             let value = parseFloat(metadatumArr[1].match(/\d+/)[0]);
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
            else if (enumFields.indexOf(metadatumArr[0]) > -1) {

                res["type"] = { value: metadatumArr[1] };
                if( metadatumArr[1] === "SCA" && metadatumArr[2] ){
                    res["sca_type"] = { value:metadatumArr[2] };
                }
                else{
                    res["sca_type"] = { value:null };
                }
            }
            else {
                res[formatMetadataFieldName(metadatumArr[0])] = {value: metadatumArr[1]};
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
 function composeCNVMetadata(cnletr) {
     if (cnletr.length < 12 || !cnletr[1]) {
         return;
     }
     let metadata = {};
     metadata.chr = {value: cnletr[1]};
     let cytobands = cnletr[2].split('-').map(Function.prototype.call, String.prototype.trim);
     metadata.cytoband_start = {value: cytobands[0]};
        // if there is only a band, stop band is just the same start band
     let cytobandStop = cytobands.length > 1 ? cytobands[1] : cytobands[0];
     metadata.cytoband_stop = {value: cytobandStop};
     metadata.start = {value: cnletr[3]};
     metadata.stop = {value: cnletr[4]};
     metadata._probes = {value: cnletr[5]};
     let ampl = parseFloat(cnletr[6].toString().replace(',','.'));
     metadata.is_amplification = {value: ampl > 0};
     metadata.amplification = {value: ampl};
     let del = parseFloat(cnletr[7].toString().replace(',','.'));
     metadata.is_deletion = {value: del < 0};
     metadata.deletion = {value: del};
     metadata.pval = {value: Number(cnletr[8].toString().replace(",",'.'))};
     let geneNames = cnletr[9] && cnletr[9].split(",").map(Function.prototype.call, String.prototype.trim);
     metadata.gene_name = {values: geneNames || []};
     let mirnas = cnletr[11] && cnletr[11].split(",").map(Function.prototype.call, String.prototype.trim);
     metadata.mirna = {values: mirnas || []};
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
     let z;
     let fileName = filePath.split("/")[filePath.split("/").length-1];
     let sampleCode = fileName.split(".")[0];
     console.log(sampleCode);
     let workbook = xlsx.readFile(filePath);
     let worksheet = workbook.Sheets[workbook.SheetNames[0]];
     let range = xlsx.utils.decode_range(worksheet['!ref']);
        //  console.log(range);
     let c, r, firstCellInRow, cell, cellElems;
     let acghProcessedFields = [];
     for (r=range.s.r; r<=range.e.r; r++) {
         firstCellInRow = worksheet[xlsx.utils.encode_cell({c:0, r:r})];
            // console.log(firstCellInRow);
         if (firstCellInRow && firstCellInRow.v && firstCellInRow.v.split) {
             if (firstCellInRow && firstCellInRow.v === 'AberrationNo') {
                 break;
             }
             cellElems = firstCellInRow.v.split(':').map(Function.prototype.call, String.prototype.trim);
                //console.log("cellElems: ", cellElems);
             acghProcessedFields.push(cellElems);
         }
         else continue;
     }
     let metadataCGHProcessed = composeCGHProcessedMetadata(acghProcessedFields);

        // Extract metadataGenomicProfile from metadataCGHProcessed
     let type=metadataCGHProcessed['type'];
     let sca_type=metadataCGHProcessed['sca_type'];
     let metadataGenomicProfile= {type,sca_type};

     delete metadataCGHProcessed['type'];
     delete metadataCGHProcessed['sca_type'];

        // Parse CNV Field Names - NOT USED now
     let cnvFieldNames = {};
     for (c=range.s.c; c<=range.e.c; c++) {
         cnvFieldNames[ worksheet[xlsx.utils.encode_cell({c:c, r:r})].v ] = c;
     }
        // console.log(cnvFieldNames);

     let cnletr = [];
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
         cnletr.push(composeCNVMetadata(cellElems));
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
     * @name composeCBInfoMetadata
     * @description update a Nb Clinical information data from an Excel file to the PostgreSQL database
     * @param{object} - patient: clinical information
     * @return{object} - object with metadata structure
     *
     */

 function composeCBInfoMetadata(patient) {
     let clinical_status, mycn, relapse, hysto, primit, ploidy, rel, evProg, evOver, inss, inrg, protocol;

     let dgnDate = moment(patient['Data di diagnosi'], 'L', 'it');
     let relDate = patient['Data_RecidivaPM'] ? moment(patient['Data_RecidivaPM'], 'L', 'it') : null;
     let FuDate = moment(patient['Data_FU_Clinico'], 'L', 'it');
     let days_Surv_Over = FuDate.diff(dgnDate, 'days');
     let days_Surv_Prog = patient['Data_RecidivaPM'] ? relDate.diff(dgnDate, 'days') : days_Surv_Over;

     ploidy = patient['DNA_Index'] && patient['DNA_Index'] !== "-9922" ? patient['DNA_Index'] : null;

     evProg = patient['Data_RecidivaPM'] ? "NO" : "YES";

     evOver = patient['Cod_Stato_FU_Clinico'] ? patient['Cod_Stato_FU_Clinico'] > 6 ? "DECEASED" : "ALIVE" : "N.D.";

     rel = patient['Data_RecidivaPM'] ? "YES" : "NO";

     inss = patient['Stadio INSS'] ? patient['Stadio INSS'] === "Non applicabile" ? "N.A." : patient['Stadio INSS'] : null;
     inss = inss && inss !== "Non so" ? inss.replace("Stadio ", "").toUpperCase() : null;

     inrg = patient['Stadio INRG'] ? patient['Stadio INRG'] === "Non applicabile" ? "N.A." : patient['Stadio INRG'] : null;
     inrg = inrg && inrg !== "Non so" ? inrg.replace("Stadio ", "").toUpperCase() : null;

     switch (patient['Cod_Stato_FU_Clinico']) {
     case "1":
         clinical_status = "ALIVE - COMPLETE REMISSION";
         break;
     case "2":
         clinical_status = "ALIVE - RESIDUAL DISEASE";
         break;
     case "3":
         clinical_status = "ALIVE - ACTIVE DISEASE";
         break;
     case "5":
         clinical_status = "ALIVE - SECOND TUMOUR";
         break;
     case "7":
         clinical_status = "DEAD FOR DISEASE";
         break;
     case "8":
         clinical_status = "DEAD FOR TOXICITY";
         break;
     case "9":
         clinical_status = "DEAD FOR OTHER REASON";
         break;
     case "10":
         clinical_status = "DEAD FOR SECOND TUMOUR";
         break;
     case "11":
         clinical_status = "DEAD FOR UNKNOWN CAUSES";
         break;
     default:
         clinical_status = null;

     }
     switch (patient['Stato_MYCN']) {
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
     switch (patient['Tipo_RecidivaPM']) {
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
     switch (patient['Istotipo']) {
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
     switch (patient['Cod_Sede tumore primitivo']) {
     case "1":
         primit = "RETROPERITONEAL GANGLIA";
         break;
     case "2":
         primit = "ABDOMEN SUPRARENAL GLANDS";
         break;
     case "3":
         primit = "ABDOMEN N.O.S.";
         break;
     case "4":
         primit = "THORAX";
         break;
     case "5":
         primit = "THORACO-ABDOMINAL";
         break;
     case "6":
         primit = "PELVIS";
         break;
     case "7":
         primit = "NECK";
         break;
     case "8":
         primit = "MULTIPLE NOT CONTIGUOUS";
         break;
     case "9":
         primit = "NOT IDENTIFIED";
         break;
     case "10":
         primit = "OTHER";
         break;
     case "-9922":
         primit = "N.D.";
         break;
     default:
         primit = null;

     }
     switch (patient['Protocollo']) {
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
         protocol = patient['Protocollo'].toUpperCase();

     }

        // logger.log('info',". dgnDate " + dgnDate._i + ". relDate " + relDate._i + ". FuDate " + FuDate._i+ ". days_Surv_Over " + days_Surv_Over+ ". days_Surv_Prog " + days_Surv_Prog);
     let metadata = {
         "inss": {
             "value": inss,
             "group": "Clinical Details"
         },
         "inrgss": {
             "value": inrg,
             "group": "Clinical Details"
         },
         "ploidy": {
             "value": ploidy,
             "group": "Biological Details"
         },
          //if data relapse_date
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
             "value": relDate!== null ? relDate.format('YYYY-MM-DD') : null,
             "group": "Clinical Details"
         },
         "relapse_type": {
             "value": relapse,
             "group": "Clinical Details"
         },
         "diagnosis_age": {
             "value": patient['Età alla diagnosi (mesi)'],
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
             "value": FuDate.format('YYYY-MM-DD'),
             "group": "Clinical Details"
         },
         "italian_nb_registry_id": {
             "value": patient['Cod_RINB'],
             "group": "Clinical Details"
         },
         "clinical_follow_up_status": {
             "value": clinical_status,
             "group": "Clinical Details"
         },
         "survival_progfree": {
             "unit": "day",
             "value": days_Surv_Prog,
             "group": "Events & Survival Info"
         },
         "survival_overall": {
             "unit": "day",
             "value": days_Surv_Over,
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

 module.exports.composeCBInfoMetadata = composeCBInfoMetadata;
 module.exports.composeCGHProcessedMetadata = composeCGHProcessedMetadata;
 module.exports.composeCNVMetadata = composeCNVMetadata;
 module.exports.getFilesInFolder = getFilesInFolder;
 module.exports.composeCGHMetadata = composeCGHMetadata;
