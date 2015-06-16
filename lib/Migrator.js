/**
 * @author Massimiliano Izzo
 * @description main tool for migration
 */
var connections = require('../config/connections.js');
var BluebirdPromise = require('bluebird');

function Migrator(mysqlConn, pgConn) {

    if (!mysqlConn) mysqlConn = 'mysqlLocal';
    if (!pgConn) pgConn = 'postgresqlLocal';

    this.knexMysql = require('knex')(connections[mysqlConn]);
    this.knexPg = require('knex')(connections[pgConn]);
}

Migrator.prototype = {
    
    /**
     * @name migrateProjects
     * @description tool to migrate all the projects
     */
    migrateProjects: function() {

        this.knexMysql.select('ID_PRJ', 'NAME_PROJECT', 'DESCR_PROJECT').from('PROJECT')
        .orderBy('ID_PRJ')

        .then(function(rows) {
            BluebirdPromise.map(rows, function(record) {
                return this.knexPg.returning('id').insert({
                    'name': record.NAME_PROJECT,
                    'description': record.DESCR_PROJECT,
                    'created_at': new Date(),
                    'updated_at': new Date() 
                }).into('project');
            }, {concurrency: 1});
        });
    
    },
    
    /**
     * @name migrateSubject
     * @description tool to migrate a signle subject together with all its data(?) in the database
     * @param{Integer} mysqlId - the ID of the subject in MySQL
     */
    migrateSubject: function(mysqlId) {
        this.knexMysql.select('NAME', 'SURNAME', 'BIRTH_DATE', 'SEX', 'CODE', 'ID_PRJ', 'INSERT_DATE', 'DATE_LAST_UPDATE').from('PERSONAL_DATA')
        .leftJoin('PATIENT', 'PERSONAL_DATA.ID_PRS_DATA', 'PATIENT.ID_PRS_DATA')
        .where('id','=',mysqlId)

        .then(function(rows) {
            var record = rows[0];
            return this.knexPg.transaction(function(trx) {
                
                // insert Personal Details 
                return this.knexPg.returning('id').insert({
                    'name': record.NAME,
                    'surname': record.SURNAME,
                    'birth_date': record.BIRTH_DATE,
                    'created_at': record.INSERT_DATE,
                    'updated_at': record.DATE_LAST_UPDATE
                }).into('personal_data').transacting(trx)
                
                // insert Subject
                .then(function(ids) {
                    var idPersonalData = ids[0];
                    return this.knexPg.returning('id').insert({
                        'code': record.CODE,
                        'sex': record.SEX,
                        'metadata': {},
                        'created_at': record.INSERT_DATE,
                        'updated_at': record.DATE_LAST_UPDATE
                    }).into('subject').transacting(trx);
                })
                
                // insert project-subject association 
                .then(function(ids) {
                    var idSubject = ids[0];
                    return this.knexPg.insert({'project_subjects': idProject, 'subject_projects': idSubject})
                    .into('project_subjects__subject_projects').transacting(trx);
                });

            });
        });
    }

};
