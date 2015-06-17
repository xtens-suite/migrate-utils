/**
 * @author Massimiliano Izzo
 * @description main tool for migration
 */
var connections = require('../config/connections.js');
var BluebirdPromise = require('bluebird');

function Migrator(mysqlConn, pgConn) {

    if (!mysqlConn) mysqlConn = 'mysqlLocal';
    if (!pgConn) pgConn = 'postgresqlLocal';

    console.log(connections[mysqlConn]);
    console.log(connections[pgConn]);

    this.knexMysql = require('knex')(connections[mysqlConn]);
    // this.knexMysql.select('ID_PRJ').from('PROJECT').then(console.log).catch(console.log);
    this.knexPg = require('knex')(connections[pgConn]);
    // this.knexPg.select('name').from('data_type').then(console.log).catch(console.log);
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
     * @name migrateCompleteSubject
     * @param {Integer} mysqlSubjId - the ID of the subject in MySQL
     */
    migrateCompleteSubject: function(mysqlSubjId) {

        var that = this;
        this.migrateSubject(mysqlSubjId)

        .then(function(idSubject) {
            console.log("Migrator.migrateCompleteSubject - created new Subject: " + idSubject);

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
                    'given_name': record.NAME,
                    'surname': record.SURNAME,
                    'birth_date': record.BIRTH_DATE,
                    'created_at': record.INSERT_DATE,
                    'updated_at': record.DATE_LAST_UPDATE
                }).into('personal_details').transacting(trx)
                
                // insert Subject
                .then(function(ids) {
                    var idPersonalData = ids[0];
                    return knexPg.returning('id').insert({
                        'code': record.CODE,
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
     * @description migrate primary samples
     */
    migrateSamples: function(mysqlSubjId, pgSubjId) {

        var knexPg = this.knexPg;
        var that = this;

        return this.knexMysql.select([
            'ID_SAMPLE, ID_SAMPLE_TYPE, BIT_CODE, ARRIVAL_CODE, AP_ARRIVAL_DATE, BM_ARRIVAL_DATE,',
            'SAMPLE_NAME, DESCR_BIT_TISSUE_NAME, DESCR_BIT_HISTOPATHOLOGY, CELLULARITY,',
            "SIZE_FIRST, SIZE_SECOND, SIZE_THIRD, CONCAT_WS(' - ', CITY, DESCR_HOSP) AS HOSP, OP_UNIT.DESCRIPTION AS UNIT, NOTES"
        ].join(""))
        .from('SAMPLE')
        .leftJoin('HOSPITAL', 'HOSPITAL.ID_HOSP', 'SAMPLE.ID_HOSP')
        .leftJoin('OP_UNIT', 'OP_UNIT.ID_OP_UNIT', 'SAMPLE.ID_OP_UNIT')
        .leftJoin('BIT_HISTOPATHOLOGY', 'BIT_HISTOPATHOLOGY.ID_BIT_HISTOPATHOLOGY', 'SAMPLE.ID_BIT_HISTOPATHOLOGY')
        .leftJoin('BIT_TISSUE_NAME', 'BIT_TISSUE_NAME.ID_BIT_TISSUE_NAME', 'SAMPLE.ID_BIT_TISSUE_NAME')
        .whereIn('ID_SAMPLE_TYPE',['TIS','FLD']).andWhere('ID_SUBJECT', mysqlSubjId).andWhereNull('ID_PARENT_SAMPLE')

        .then(function(rows) {
            console.log("Migrator.migrateSamples: got these samples: " + rows);
            // save each (primary) sample
            return BluebirdPromise.each(rows, function(sample) {
                return that.createPrimary(sample, pgSubjId);    
            });

        });
    
    },

    createPrimary: function(sample, idSubj) {

        var knexPg = this.knexPg;
        var sampleType;
        var metadata = {
            arrival_date_mb: {value: sample.BM_ARRIVAL_DATE},
            sample_name: {value: sample.SAMPLE_NAME},
            sample_codification: {value: sample.DESCR_BIT_TISSUE_NAME},
            pathology: {value: sample.DESCR_BIT_HISTOPATHOLOGY},
            hospital: {value: sample.HOSP},
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
                sampleType = 'Tissue';
                break;
            case 'FLD':
                metadata.sampling_date = {value: sample.AP_ARRIVAL_DATE};
                metadata.quantity = {value: sample.QUANTITY, unit: 'millilitre'};
                sampleType = 'Fluid';
        }
        
        console.log("Migrator.createPrimary - metadata: " + metadata);
        return knexPg.select('id').from('sample').where('name', sampleType)

        .then(function(rows) {
            if (rows && rows.length) {
                return knexPg.returning('id').insert({
                    'type': rows[0].id,
                    'biobank': 1,       // NOTA BENE: there is only one biobank!!
                    'metadata': metadata,
                    'biobank_code': sample.BIT_CODE,
                    'parent_subject': idSubj,
                    'created_at': new Date(),
                    'updated_at': new Date()
                }); 
            }
        });
        
    }

};

module.exports = Migrator;
