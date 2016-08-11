module.exports = {

    bearerToken: 'eyJhbGciOiJIUzI1NiJ9.eyJpZCI6NywiaXNXaGVlbCI6dHJ1ZSwiaXNBZG1pbiI6dHJ1ZSwiY2FuQWNjZXNzUGVyc29uYWxEYXRhIjp0cnVlLCJjYW5BY2Nlc3NTZW5zaXRpdmVEYXRhIjp0cnVlfQ.Tf57NjL4LJ1iHqrYfO9KzBYlNbjUBDEK00uR2YlazQg',

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
    }

};
