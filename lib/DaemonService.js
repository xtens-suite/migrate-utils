/**
 * DaemonService
 *
 * @description :: Server-side logic for managing data
 * @help        :: See http://links.sailsjs.org/docs/controllers
 */
/* jshint node: true */

"use strict";

const basePath = 'http://localhost:1337';
const BluebirdPromise = require('bluebird');
const request = BluebirdPromise.promisify(require("request"));
const _ = require("lodash");
const http = require("http");
const connections = require('../config/connections.js');

const INITIALIAZING = "initializing";
const ERROR = "error";
const RUNNING = "running";
const SUCCESS = "success";

BluebirdPromise.promisifyAll(request, {
    multiArgs: true
});

const coroutines = {

    /**
     * @method
     * @name initialize
     * @param{Request} req
     * @param{Response} res
     * @description coroutine for new Data instance creation
     */
    InitializeDeamon: BluebirdPromise.coroutine(function *(source, infoObj, operator, processInfo, token) {
        let info = {
            totalRows: infoObj.totalRows ? infoObj.totalRows : 0,
            processedRows: infoObj.processedRows ? infoObj.processedRows : 0,
            notProcessedRows: infoObj.notProcessedRows ? infoObj.notProcessedRows : [],
            percentage: infoObj.totalRows && infoObj.processedRows ? infoObj.processedRows / infoObj.totalRows : 0,
            error: ""
        };
        let daemon = {
            pid: processInfo.pid,
            source: source,
            operator: operator,
            info: info,
            status: INITIALIAZING,
            createdAt: new Date(),
            updatedAt: new Date()
        };

        let [resCreate,bodyCreate] = yield request.postAsync({
            uri: basePath + '/daemon',
            auth: {
                bearer: token
            },
            json: daemon
        });
        return bodyCreate;
    }),

    UpdateDeamon: BluebirdPromise.coroutine(function *(daemon, token) {

        daemon.info.percentage = daemon.info.totalRows !== 0 || daemon.info.processedRows ? Math.round(daemon.info.processedRows / daemon.info.totalRows * 100) : 0;
        daemon.status = RUNNING;
        daemon.updatedAt = new Date();

        let [resUpdate,bodyUpdate] = yield request.putAsync({
            uri: basePath + '/daemon/' + daemon.id,
            auth: {
                bearer: token
            },
            json: daemon
        });
        return bodyUpdate;
    }),

    ErrorDeamon: BluebirdPromise.coroutine(function *(daemon, error, token) {
        daemon.info.error = error ? error : new Error("Get some error during customised data Import" + daemon.source ? ": " + daemon.source : "");
        daemon.status = ERROR;
        daemon.updatedAt = new Date();

        let [resUpdate,bodyUpdate] = yield request.putAsync({
            uri: basePath + '/daemon/' + daemon.id,
            auth: {
                bearer: token
            },
            json: daemon
        });

        return bodyUpdate;
    }),

    SuccessDeamon: BluebirdPromise.coroutine(function *(daemon, token) {
        var now = new Date();
        var elapsedTime = Math.round((now.getTime() - new Date(daemon.createdAt).getTime()) / (1000));   // in seconds
        daemon.info.elapsedTime = elapsedTime;
        daemon.info.percentage = daemon.info.totalRows !== 0 || daemon.info.processedRows ? Math.round(daemon.info.processedRows / daemon.info.totalRows * 100) : 0;
        daemon.status = SUCCESS;
        daemon.updatedAt = new Date();

        let [resUpdate,bodyUpdate] = yield request.putAsync({
            uri: basePath + '/daemon/' + daemon.id,
            auth: {
                bearer: token
            },
            json: daemon
        });
        return bodyUpdate;
    })
};


module.exports = {

  /**
   * @method
   * @name InitializeDeamon
   * @description initialize the daemon object
   * @param{Array} source - an array of names of all processed files
   * @param{Object} infoObj - an object containing all info related to the data insertion
   * @param{Integer} operator - Id operator
   * @param{Object} processInfo - an object containing all info related to the data insertion
   * @param{string} token - the operator's bearer token
   */
    InitializeDeamon: function(source, infoObj, operator, processInfo, token) {
        return coroutines.InitializeDeamon(source, infoObj, operator, processInfo, token)
      .catch(function(err) {
          throw err;
      });
    },



    /**
     * @method
     * @name UpdateDeamon
     * @description update the daemon object with the new info
     * @param{string} dameon - the daemon object to be updated
     * @param{string} token - the operator's bearer token

     */
    UpdateDeamon: function(daemon, token) {
        return coroutines.UpdateDeamon(daemon, token)
        .catch(function(err) {
            throw err;
        });
    },


    /**
    * @method
    * @name ErrorDeamon
    * @description update the daemon object status with the related message error
    * @param{string} dameon - the daemon object to be updated
    * @param{string} error - an error object
    * @param{string} token - the operator's bearer token

    */
    ErrorDeamon: function(daemon, error, token) {
        return coroutines.ErrorDeamon(daemon, error, token)
      .catch(function(err) {
          throw err;
      });
    },


    /**
    * @method
    * @name SuccessDeamon
    * @description update the daemon object setting status to success and uploading the related info
    * @param{string} dameon - the daemon object to be updated
    * @param{string} token - the operator's bearer token
    */
    SuccessDeamon: function(daemon, token) {
        return coroutines.SuccessDeamon(daemon, token)
      .catch(function(err) {
          throw err;
      });
    }
};
