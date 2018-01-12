module.exports = {

    bearerToken: '',

    mysqlLocal: {
        client: 'mysql',
        connection: {
            host: '127.0.0.1',
            user: 'root',
            password: 'root',
            database: 'xtensMigrate'
        }
    },

    postgresqlLocal: {
        client: 'pg',
        connection: {
            host: '127.0.0.1',
            port: 5432,
            user: 'xtenspg',
            password: 'xtenspg',
            database: 'xtensigg'
        }
    },

    postgresqlLocalAnnotiation: {
        client: 'pg',
        connection: {
            host: '127.0.0.1',
            port: 5432,
            user: 'xtenspg',
            password: 'xtenspg',
            database: 'xtens_genomic_annotation'
        }
    }

};
