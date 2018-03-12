/**
 * Created by hyc on 18-2-28
 */

"use strict";
let assert = require('assert');
let pegasusClient = require('../');
let Exception = require('../src/errors');
let ErrorType = require('../src/dsn/base_types').error_type;

describe('test/client.test.js', function(){
    this.timeout(10000);
    let client = null, tableName = 'temp';

    before(function(){
        client = pegasusClient.create({
            metaList: ['127.0.0.1:34601', '127.0.0.1:34602', '127.0.0.1:34603'],
            rpcTimeOut : 5000,
        });
    });
    after(function(){
        client.close();
    });

    describe('get table', function(){
        it('ok', function(done){
            client.getTable(tableName, function(err, tableInfo){
                assert.equal(null, err);
                done();
            });
        });
        it('wrong table name', function(done){
            client.getTable('404', function(err, tableInfo){
                assert.equal(ErrorType.ERR_OBJECT_NOT_FOUND, err.err_code);
                assert.equal(null, tableInfo);
                //console.log(err.message);
                done();
            });
        });

    });

    describe('set', function(){
        it('simple set', function(done){
            let args = {
                'hashKey' : '1',
                'sortKey' : '1',
                'value'   : '1',
                'timeout' : 500,
            };
            client.set(tableName, args, function(err){
                assert.equal(null, err);
                done();
            });
        });
    });

    describe('get', function(){
        it('simple get', function(done){
            let args = {
                'hashKey' : '1',
                'sortKey' : '1',
            };
            client.get(tableName, args, function(err, result){
                assert.equal(null, err);
                assert.equal('1', result);
                //console.log('result is %s', result);
                done();
            });
        });
        it('buffer params', function(done){
            let args = {
                'hashKey' : new Buffer('1'),
                'sortKey' : new Buffer('1'),
            };
            client.get(tableName, args, function(err, result){
                assert.equal(null, err);
                assert.deepEqual(new Buffer('1'), result);
                //console.log('result is %s', result);
                done();
            });
        });

        it('no value', function(done){
            let args = {
                'hashKey' : '404',
                'sortKey' : 'not-found',
            };
            client.get(tableName, args, function(err, result){
                assert.equal(null, err);
                assert.equal('', result);
                done();
            });
        });

        it('wrong table' ,function(done){
            let args = {
                'hashKey' : '1',
                'sortKey' : '1',
            };
            client.get('404', args, function(err, result){
                assert.equal(null, result);
                assert(err instanceof Exception.MetaException);
                assert.equal(ErrorType.ERR_OBJECT_NOT_FOUND, err.err_code);
                //console.log(err.message);
                done();
            });
        });
    });

    describe('batch set', function(){
        it('simple batch set', function(done){
            let argArray = [];
            argArray[0] = {
                'hashKey' : '1',
                'sortKey' : '11',
                'value'   : '11',
                'timeout' : 300,
            };
            argArray[1] = {
                'hashKey' : '1',
                'sortKey' : '22',
                'value'   : '22',
                'timeout' : 300,
            };
            client.batchSet(tableName, argArray, function(err){
                assert.equal(null, err);
                done();
            });
        });
    });

    describe('batch get', function(){
        it('simple batch get', function(done){
            let argArray = [];
            argArray[0] = {
                'hashKey' : '1',
                'sortKey' : '11',
                'timeout' : 200,
            };
            argArray[1] = {
                'hashKey' : '1',
                'sortKey' : '22',
                'timeout' : 200,
            };
            client.batchGet(tableName, argArray, function(err, result){
                assert.equal(null, err);
                assert.equal(2, result.length);
                assert.equal('1', result[0].hashKey);
                assert.equal('11', result[0].sortKey);
                assert.equal('11', result[0].value);
                assert.equal('1', result[1].hashKey);
                assert.equal('22', result[1].sortKey);
                assert.equal('22', result[1].value);
                // assert.equal('11', result[0]);
                // assert.equal('22', result[1]);
                done();
            });
        });
    });

    describe('multi set', function(){
        it('simple multi set', function(done){
            let array = [];
            array[0] = {
                'key' : '11',
                'value' : '111',
            };
            array[1] = {
                'key' : '22',
                'value' : '222',
            };


            let args = {
                'hashKey' : '1',
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
                'hashKey' : '1',
                'sortKeyArray' : ['1', '11', '22'],
            };
            client.multiGet(tableName, args, function(err, result){
                assert.equal(null, err);
                assert.equal('1', result[0].value.data);
                assert.equal('111', result[1].value.data);
                assert.equal('222', result[2].value.data);
                done();
            });
        });
    });

    describe('delete', function(){
        it('simple delete', function(done){
            let args = {
                'hashKey' : '1',
                'sortKey' : '22',
            };
            client.del(tableName, args, function(err){
                assert.equal(null, err);
                done();
            });
        });
    });
});
