/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

"use strict";
let assert = require('assert');
let pegasusClient = require('../');
let PException = require('../src/errors').PException;
let ErrorType = require('../src/dsn/dsn_types').error_type;
let log4js = require('log4js');

describe('test/client.test.js', function(){
    this.timeout(10000);
    let client = null, tableName = 'temp';

    log4js.configure({
        appenders: { pegasus: { type: 'stdout'} },
        categories: { default: { appenders: ['pegasus'], level: 'INFO' } }
    });
    let log = log4js.getLogger('pegasus');

    before(function(){
        client = pegasusClient.create({
            metaServers: ['127.0.0.1:34601', '127.0.0.1:34602', '127.0.0.1:34603'],
            operationTimeout : 5000,
            log : log,
        });
    });
    after(function(){
        client.close();
    });

    describe('create client failure', function(){
        it('lack of metaServers', function(done){
            try{
                pegasusClient.create({});
            }catch(e){
                assert(e instanceof PException);
                console.log(e.message);
            }
            done();
        });

        it('invalid ip:port', function(done){
            try{
                pegasusClient.create({metaServers: ['127.0.0.1.34601', '127.0.0.1:34602']});
            }catch(e){
                assert(e instanceof PException);
                console.log(e.message);
            }
            done();
        });

        it('invalid ipv4 address', function(done){
            try{
                pegasusClient.create({metaServers: ['127.0.0.1:34601', '485.0.0.1:34602']});
            }catch(e){
                assert(e instanceof PException);
                console.log(e.message);
            }
            done();
        });

        it('invalid ipv4 address', function(done){
            try{
                pegasusClient.create({metaServers: ['127.0.0.1:34601', '127.0.0.1:99999']});
            }catch(e){
                assert(e instanceof PException);
                console.log(e.message);
            }
            done();
        });
    });

    describe('get table', function(){
        it('ok', function(done){
            client.getTable(tableName, function(err){
                assert.equal(null, err);
                done();
            });
        });

        it('invalid table name', function(done){
            client.getTable(123654, function(err, tableInfo){
                assert(err instanceof PException);
                assert.equal(null, tableInfo);
                console.log(err.message);
                done();
            });
        });

        it('wrong table name', function(done){
            client.getTable('404', function(err, tableInfo){
                assert.equal(ErrorType.ERR_OBJECT_NOT_FOUND, err.err_code);
                assert.equal(null, tableInfo);
                done();
            });
        });

    });

    describe('set', function(){
        it('simple set', function(done){
            let args = {
                'hashKey' : new Buffer('1'),
                'sortKey' : new Buffer('1'),
                'value'   : new Buffer('1'),
                'timeout' : 5000,
            };
            client.set(tableName, args, function(err){
                assert.equal(null, err);
                done();
            });
        });
        it('invalid param', function(done){
            let args = {
                'hashKey' : '1',
                'sortKey' : '1',
                'value'   : '1',
                'timeout' : 5000,
            };
            client.set(tableName, args, function(err){
                assert(err instanceof PException);
                console.log(err.message);
                done();
            });
        });
    });

    describe('get', function(){
        it('simple get', function(done){
            let args = {
                'hashKey' : new Buffer('1'),
                'sortKey' : new Buffer('1'),
            };
            client.get(tableName, args, function(err, result){
                assert.equal(null, err);
                assert.deepEqual(new Buffer('1'), result.hashKey);
                assert.deepEqual(new Buffer('1'), result.sortKey);
                assert.deepEqual(new Buffer('1'), result.value);
                done();
            });
        });

        it('no value', function(done){
            let args = {
                'hashKey' : new Buffer('404'),
                'sortKey' : new Buffer('not-found'),
            };
            client.get(tableName, args, function(err, result){
                assert.equal(null, err);
                assert.deepEqual(new Buffer(''), result.value);
                done();
            });
        });
    });

    describe('batch set', function(){
        it('simple batch set', function(done){
            let argArray = [];
            argArray[0] = {
                'hashKey' : new Buffer('1'),
                'sortKey' : new Buffer('11'),
                'value'   : new Buffer('11'),
                'timeout' : 3000,
            };
            argArray[1] = {
                'hashKey' : new Buffer('1'),
                'sortKey' : new Buffer('22'),
                'value'   : new Buffer('22'),
                'timeout' : 3000,
            };
            client.batchSet(tableName, argArray, function(err){
                assert.equal(null, err);
                done();
            });
        });

        it('lack param array', function(done){
            let argArray = {};
            client.batchSet(tableName, argArray, function(err){
                assert(err instanceof PException);
                console.log(err.message);
                done();
            });
        });

        it('wrong param', function(done){
            let argArray = [];
            argArray[0] = {
                'hashKey' : new Buffer('1'),
                'sortKey' : new Buffer('11'),
                'value'   : new Buffer('11'),
                'timeout' : 3000,
            };
            argArray[1] = {
                'hashKey' : new Buffer('1'),
                'sortKey' : new Buffer('22'),
                'value'   : '22',
                'timeout' : 3000,
            };
            client.batchSet(tableName, argArray, function(err){
                assert(err instanceof PException);
                console.log(err.message);
                done();
            });
        });
    });

    describe('batch get', function(){
        it('simple batch get', function(done){
            let argArray = [];
            argArray[0] = {
                'hashKey' : new Buffer('1'),
                'sortKey' : new Buffer('11'),
                'timeout' : 2000,
                'maxFetchCount' : 100,
                'maxFetchSize'  : 1000000
            };
            argArray[1] = {
                'hashKey' : new Buffer('1'),
                'sortKey' : new Buffer('22'),
                'timeout' : 2000,
                'maxFetchCount' : 100,
                'maxFetchSize'  : 1000000
            };
            client.batchGet(tableName, argArray, function(err, result){
                assert.equal(null, err);
                assert.equal(2, result.length);
                assert.deepEqual(new Buffer('1'), result[0].data.hashKey);
                assert.deepEqual(new Buffer('11'), result[0].data.sortKey);
                assert.deepEqual(new Buffer('11'), result[0].data.value);
                assert.deepEqual(new Buffer('1'), result[1].data.hashKey);
                assert.deepEqual(new Buffer('22'), result[1].data.sortKey);
                assert.deepEqual(new Buffer('22'), result[1].data.value);
                done();
            });
        });
    });

    describe('multi set', function(){
        it('simple multi set', function(done){
            let array = [];
            array[0] = {
                'key' : new Buffer('11'),
                'value' : new Buffer('111'),
            };
            array[1] = {
                'key' : new Buffer('22'),
                'value' : new Buffer('222'),
            };


            let args = {
                'hashKey' : new Buffer('1'),
                'sortKeyValueArray' : array,
            };
            client.multiSet(tableName, args, function(err){
                assert.equal(null, err);
                done();
            });
        });
    });


    describe('multi get', function(){
        it('simple multi get', function(done){
            let args = {
                'hashKey' : new Buffer('1'),
                'sortKeyArray' : [
                    new Buffer('1'),
                    new Buffer('11'),
                    new Buffer('22'),
                ],
            };
            client.multiGet(tableName, args, function(err, result){
                assert.equal(null, err);
                assert.deepEqual(new Buffer('1'), result[0].hashKey);
                assert.deepEqual(new Buffer('1'), result[0].sortKey);
                assert.deepEqual(new Buffer('1'), result[0].value);
                assert.deepEqual(new Buffer('11'), result[1].sortKey);
                assert.deepEqual(new Buffer('111'), result[1].value);
                assert.deepEqual(new Buffer('22'), result[2].sortKey);
                assert.deepEqual(new Buffer('222'), result[2].value);
                done();
            });
        });

        it('multi get all sortKey-value', function(done){
            let args = {
                'hashKey' : new Buffer('1'),
                'sortKeyArray' : [],
            };
            client.multiGet(tableName, args, function(err, result){
                assert.equal(null, err);
                assert.deepEqual(new Buffer('1'), result[0].hashKey);
                assert.deepEqual(new Buffer('1'), result[0].sortKey);
                assert.deepEqual(new Buffer('1'), result[0].value);
                assert.deepEqual(new Buffer('11'), result[1].sortKey);
                assert.deepEqual(new Buffer('111'), result[1].value);
                assert.deepEqual(new Buffer('22'), result[2].sortKey);
                assert.deepEqual(new Buffer('222'), result[2].value);
                done();
            });
        });
    });

    describe('delete', function(){
        it('simple delete', function(done){
            let args = {
                'hashKey' : new Buffer('1'),
                'sortKey' : new Buffer('22'),
            };
            client.del(tableName, args, function(err){
                assert.equal(null, err);
                done();
            });
        });
    });
});
