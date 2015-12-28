module.exports = {

    bearerToken: 'eyJhbGciOiJIUzI1NiJ9.eyJpZCI6NywiaXNXaGVlbCI6dHJ1ZSwiaXNNYW5hZ2VyIjp0cnVlLCJjYW5BY2Nlc3NQZXJzb25hbERhdGEiOnRydWUsImNhbkFjY2Vzc1NlbnNpdGl2ZURhdGEiOnRydWV9.5KkO2NM5BpKH7ChlfGb5iACVytZkNqySp_x9dt0OACU',

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
