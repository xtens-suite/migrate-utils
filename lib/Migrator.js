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
     * @name migrateSubject
     * @description tool to migrate a signle subject together with all its data(?) in the database
     * @param{Integer} mysqlId - the ID of the subject in MySQL
     */
    migrateSubject: function(mysqlId) {

        var knexPg = this.knexPg;

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
                    var idSubject = ids[0];
                    return knexPg.insert({'project_subjects': idProject, 'subject_projects': idSubject})
                    .into('project_subjects__subject_projects').transacting(trx);
                });

            });
        });
    }

};

module.exports = Migrator;
