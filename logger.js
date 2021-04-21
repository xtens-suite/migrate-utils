/* eslint-disable func-style */
/* jshint esnext: true */
/* jshint node: true */
"use strict";
// var fs = require('fs');
// var util = require('util');
var winston = require('winston');
// let logger = {};
// var log_file = fs.createWriteStream(__dirname + '/logs/debug.log', {flags : 'a'});
// var log_stdout = process.stdout;

// logger.log = function(d,f) {
//   const  dt = new Date().toUTCString();
//   log_file.write(dt + " " + util.format(d.toString() + " - " + f.toString()) + '\n');
//   log_stdout.write(dt + " " + util.format(d.toString() + " - " + f.toString()) + '\n');
// };

// singleton
var logger;

const createLogger = () => {
    if (logger) {
        return logger;
    }

    logger = winston.createLogger({
        // level: 'info',
        format: winston.format.combine(
            winston.format.timestamp(),
            winston.format.prettyPrint()
        ), //   defaultMeta: { service: 'user-service' },
        transports: [
            //
            // - Write all logs with level `error` and below to `error.log`
            // - Write all logs with level `info` and below to `combined.log`
            //
            new winston.transports.File({ filename: './logs/MigrateUtils/error.log', level: 'error' }),
            new winston.transports.File({ filename: './logs/MigrateUtils/combined.log', level: 'info' })
        ]
    });

    //
    // If we're not in production then log to the `console` with the format:
    // `${info.level}: ${info.message} JSON.stringify({ ...rest }) `
    //
    if (process.env.NODE_ENV !== 'production') {
        logger.add(new winston.transports.Console({
            format: winston.format.simple()
        }));
    }
    return logger;
};

module.exports = createLogger;
