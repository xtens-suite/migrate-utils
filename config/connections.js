module.exports = {

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
            port: 9432,
            user: 'xtenspg',
            password: 'xtenspg',
            database: 'xtensigg'
        }
    }

};
