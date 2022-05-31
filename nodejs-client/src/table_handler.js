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

const Blob = require('./dsn/base_types').blob;
const Gpid = require('./dsn/base_types').gpid;
const ErrorType = require('./dsn/base_types').error_type;
const RrdbType = require('./dsn/rrdb_types');
const replica = require('./dsn/replication_types');
const Exception = require('./errors');
const ReplicaSession = require('./session').ReplicaSession;
const MetaRequestRound = require('./session').MetaRequestRound;
const ClientRequestRound = require('./session').ClientRequestRound;
const Operator = require('./operator');
const Long = require('long');
const tools = require('./tools');

const DEFAULT_MULTI_COUNT = 100;
const DEFAULT_MULTI_SIZE = 1000000;
let log = null;

/**
 * Constructor of tableHandle
 * @param   {Cluster}   cluster
 * @param   {String}    tableName
 * @param   {Function}  callback
 * @constructor
 */
function TableHandler(cluster, tableName, callback) {
    log = cluster.log;
    this.cluster = cluster;
    this.tableName = tableName;
    this.keyHash = new PegasusHash();
    this.queryCfgResponse = null;
    this.callback = callback;

    this.app_id = 0;
    this.partition_count = 0;
    this.partitions = {};       //partition pid -> primary rpc_address
    this.ballots = {};          //partition pid -> ballot

    if (this.cluster.metaSession.connectionError) {
        //Failed to get meta sessions
        callback(this.cluster.metaSession.connectionError, null);
    } else {
        this.queryMeta(tableName, this.onUpdateResponse.bind(this));
    }
}

/**
 * create operator and round object, and send request to session
 * @param {String}      tableName
 * @param {Function}    callback
 */
TableHandler.prototype.queryMeta = function (tableName, callback) {
    let session = this.cluster.metaSession;
    let index = session.curLeader;
    let leader = session.metaList[index];

    // connection has error or closed
    if (leader.connectError || leader.closed) {
        session.handleConnectedError(index);
        session.queryingTableName[tableName] = false;
        session.lastQueryTime[tableName] = 0;
    }

    let queryCfgOperator = new Operator.QueryCfgOperator(new Gpid(-1, -1),
        new replica.query_cfg_request({'app_name': tableName}),
        this.cluster.timeout);
    let round = new MetaRequestRound(queryCfgOperator,
        callback,
        session.maxRetryCounter,
        index,
        leader);

    let isQueryingMeta = session.queryingTableName[tableName];
    let lastQueryTime = session.lastQueryTime[tableName] ? session.lastQueryTime[tableName] : 0;
    if(!isQueryingMeta && Date.now() - lastQueryTime > this.cluster.queryMetaDelay){
        session.queryingTableName[tableName] = true;
        session.lastQueryTime[tableName] = Date.now();
        session.query(round);
        log.debug('table %s start to query meta', tableName);
    } else if (isQueryingMeta ){
        log.debug('table %s already exectute querying meta, ignore this request', tableName);
    } else {
        log.debug('table %s query meta too frequently, ignore this request', tableName);
    }
};

/**
 * Handle err and response
 * @param {Error}       err
 * @param {Operator}    op
 */
TableHandler.prototype.onUpdateResponse = function (err, op) {
    let response = op.response;

    if (err === null && ErrorType[response.err.errno] === ErrorType.ERR_OK) {
        this.updateResponse(this.queryCfgResponse, response);
        this.callback(null, this);
    } else if (err === null) {
        this.callback(new Exception.MetaException(
            response.err.errno, 'Failed to query meta server, error is ' + response.err.errno
        ), null);
    } else {
        this.callback(err, null);
    }
};

/**
 * Update current config according to metaCfgResponse
 * @param   oldResp
 * @param   newResp
 */
TableHandler.prototype.updateResponse = function (oldResp, newResp) {
    this.app_id = newResp.app_id;
    this.partition_count = newResp.partition_count;

    let current_sessions = this.cluster.replicaSessions;
    this.partitions = [];
    this.ballots = [];
    this.cluster.ReplicaSessions = [];
    let connected_rpc = [];

    let i, len = newResp.partitions.length;
    for (i = 0; i < len; ++i) {
        let partition = newResp.partitions[i];
        let primary_addr = partition.primary;
        this.partitions[partition.pid.get_pidx()] = primary_addr;
        this.ballots[partition.pid.get_pidx()] = partition.ballot;

        // table is partition split, and child partition is not ready
        // child requests should be redirected to its parent partition
        // this will be happened when query meta is called during partition split
        if (partition.ballot < 0) {
            continue;
        }
        if (tools.isAddrExist(connected_rpc, primary_addr) === true) {
            continue;
        }
        let session = tools.findSessionByAddr(current_sessions, primary_addr);
        if (session === null) {
            session = new ReplicaSession({'address': primary_addr});
            this.cluster.replicaSessions.push({
                'key': primary_addr,
                'value': session,
            });
        }
        connected_rpc.push(primary_addr);
    }
    this.queryCfgResponse = newResp;
    log.info('table %s finish updating config from meta', this.tableName);
};

/**
 * Create round and send request to session
 * @param {gpid}        gpid
 * @param {Operator}   op
 */
TableHandler.prototype.operate = function (gpid, op) {
    let pidx = gpid.get_pidx();
    let address = this.partitions[pidx];
    let session = tools.findSessionByAddr(this.cluster.replicaSessions, address);

    if (session === null || session === undefined) {
        log.error('Replica session not exist');
        return;
    }

    // connection has error or closed
    if (session.connection.connectError || session.connection.closed) {
        session.handleConnectedError(address);
    }

    let clientRound = new ClientRequestRound(this, op, op.handleResult.bind(op));
    session.operate(clientRound, address);
};

/**
 * Constructor of TableInfo
 * @param   {Client}        client
 * @param   {TableHandler}  tableHandler
 * @param   {Number}        timeout
 * @constructor
 */
function TableInfo(client, tableHandler, timeout) {
    this.client = client;
    this.tableHandler = tableHandler;
    this.timeout = timeout;
}

/**
 * Create Gpid object by hash_value
 * @param   {Long}  hash_value
 * @return  {gpid}  gpid
 */
TableInfo.prototype.getGpidByHash = function(hash_value) {
    let app_id = this.tableHandler.app_id;
    let pidx = hash_value.mod(this.tableHandler.partition_count).getLowBits();
    //pidx should be greater than zero
    while (pidx < 0 && pidx < this.tableHandler.partition_count) {
        pidx += this.tableHandler.partition_count;
    }
    // table is partition split, and child partition is not ready
    // child requests should be redirected to its parent partition
    if(this.tableHandler.ballots[pidx] < 0) {
        log.info('table[%s] is executing partition split, partition[%d] is not ready, requests will send to parent partition[%d]', 
            this.tableHandler.tableName,
            pidx,
            pidx - this.tableHandler.partition_count / 2);
        pidx -= this.tableHandler.partition_count / 2;
    }

    return new Gpid({
        'app_id': app_id,
        'pidx': pidx,
    });
};

/**
 * Get value
 * @param {Object}      args
 *        {Buffer}      args.hashKey
 *        {Buffer}      args.sortKey
 *        {Number}      args.timeout
 * @param {Function}    callback
 */
TableInfo.prototype.get = function (args, callback) {
    let timeout = args.timeout || this.timeout;
    let request = new Blob({
        'data': tools.generateKey(args.hashKey, args.sortKey),
    });
    let hash_value = this.tableHandler.keyHash.hash(request.data);
    let gpid = this.getGpidByHash(hash_value);
    let op = new Operator.RrdbGetOperator(gpid, request, args.hashKey, args.sortKey, hash_value, timeout, callback);
    this.tableHandler.operate(gpid, op);
};

/**
 * Batch Get value using promise
 * @param {Array}       argsArray
 *        {Buffer}      argsArray[i].hashKey
 *        {Buffer}      argsArray[i].sortKey
 *        {Number}      argsArray[i].timeout(ms)
 * @param {Function}    callback
 */
TableInfo.prototype.batchGetPromise = function (argsArray, callback) {
    let tasks = [], i, len = argsArray.length, self = this;
    for (i = 0; i < len; ++i) {
        tasks.push(new Promise(function (resolve) {
            self.get(argsArray[i], function (err, result) {
                resolve({'error': err, 'data': result});
            });
        }));
    }
    Promise.all(tasks).then(function (values) {
        callback(null, values);
    });
};


/**
 * Set value
 * @param {Object}      args
 *        {Buffer}      args.hashKey
 *        {Buffer}      args.sortKey
 *        {Buffer}      args.value
 *        {Number}      args.ttl
 *        {Number}      args.timeout
 * @param {Function}    callback
 */
TableInfo.prototype.set = function (args, callback) {
    //set ttl
    if (!args.ttl || typeof(args.ttl) !== 'number' || args.ttl < 0) {
        args.ttl = 0;
    }
    let timeout = args.timeout || this.timeout;
    let key = new Blob({
        'data': tools.generateKey(args.hashKey, args.sortKey),
    });
    let blob_value = new Blob({
        'data': args.value,
    });

    let hash_value = this.tableHandler.keyHash.hash(key.data);
    let gpid = this.getGpidByHash(hash_value);

    let op = new Operator.RrdbPutOperator(gpid, new RrdbType.update_request({
        'key': key,
        'value': blob_value,
        'expire_ts_seconds': args.ttl,
    }), hash_value, timeout, callback);
    this.tableHandler.operate(gpid, op);
};

/**
 * Batch Set value using promise
 * @param {Array}       argsArray
 *        {Buffer}      argsArray[i].hashKey
 *        {Buffer}      argsArray[i].sortKey
 *        {Buffer}      argsArray[i].value
 *        {Number}      argsArray[i].ttl
 *        {Number}      argsArray[i].timeout(ms)
 * @param {Function}    callback
 */
TableInfo.prototype.batchSetPromise = function (argsArray, callback) {
    let tasks = [], i, len = argsArray.length, self = this;
    for (i = 0; i < len; ++i) {
        tasks.push(new Promise(function (resolve) {
            self.set(argsArray[i], function (err, result) {
                resolve({'error': err, 'data': result});
            });
        }));
    }
    Promise.all(tasks).then(function (values) {
        callback(null, values);
    });
};

/**
 * Delete value
 * @param {Object}      args
 *        {Buffer}      args.hashKey
 *        {Buffer}      args.sortKey
 *        {Number}      args.timeout
 * @param {Function}    callback
 */
TableInfo.prototype.del = function (args, callback) {
    let timeout = args.timeout || this.timeout;
    let request = new Blob({
        'data': tools.generateKey(args.hashKey, args.sortKey),
    });

    let hash_value = this.tableHandler.keyHash.hash(request.data);
    let gpid = this.getGpidByHash(hash_value);
    let op = new Operator.RrdbRemoveOperator(gpid, request, hash_value, timeout, callback);
    this.tableHandler.operate(gpid, op);
};

/**
 * Multi Get
 * @param {Object}      args
 *        {Buffer}      args.hashKey
 *        {Array}       args.sortKeyArray
 *        {Number}      args.timeout(ms)
 *        {Number}      args.maxFetchCount
 *        {Number}      args.maxFetchSize
 * @param {Function}    callback
 */
TableInfo.prototype.multiGet = function (args, callback) {
    let timeout = args.timeout || this.timeout,
        maxFetchCount = args.maxFetchCount || DEFAULT_MULTI_COUNT,
        maxFetchSize = args.maxFetchSize || DEFAULT_MULTI_SIZE,
        no_value = false;

    let hash_value = this.tableHandler.keyHash.default_hash(args.hashKey);
    let gpid = this.getGpidByHash(hash_value);

    let hashKeyBlob = new Blob({
        'data': args.hashKey,
    });
    let i, len = args.sortKeyArray.length, sortKeyBlobs = [];
    for (i = 0; i < len; ++i) {
        sortKeyBlobs[i] = new Blob({
            'data': args.sortKeyArray[i],
        });
    }
    let request = new RrdbType.multi_get_request({
        'hash_key': hashKeyBlob,
        'sork_keys': sortKeyBlobs,
        'max_kv_count': maxFetchCount,
        'max_kv_size': maxFetchSize,
        'no_value': no_value
    });
    let op = new Operator.RrdbMultiGetOperator(gpid, request, args.hashKey, hash_value, timeout, callback);
    this.tableHandler.operate(gpid, op);
};

/**
 * Multi Get
 * @param {Object}      args
 *        {Buffer}      args.hashKey
 *        {Array}       args.sortKeyValueArray
 *                      {'key' : sortKey, 'value' : value}
 *        {Number}      args.timeout(ms)
 *        {Number}      args.ttl(s)
 * @param {Function}    callback
 */
TableInfo.prototype.multiSet = function (args, callback) {
    if (!args.ttl || typeof(args.ttl) !== 'number' || args.ttl < 0) {
        args.ttl = 0;
    }
    let timeout = args.timeout || this.timeout,
        hash_value = this.tableHandler.keyHash.default_hash(args.hashKey),
        gpid = this.getGpidByHash(hash_value);

    let hashKeyBlob = new Blob({
        'data': args.hashKey,
    });
    let i, len = args.sortKeyValueArray.length, valueBlobs = [];
    for (i = 0; i < len; ++i) {
        valueBlobs[i] = new RrdbType.key_value({
            'key': new Blob({'data': args.sortKeyValueArray[i].key}),
            'value': new Blob({'data': args.sortKeyValueArray[i].value}),
        });
    }

    let request = new RrdbType.multi_put_request({
        'hash_key': hashKeyBlob,
        'kvs': valueBlobs,
        'expire_ts_seconds': args.ttl,
    });

    let op = new Operator.RrdbMultiPutOperator(gpid, request, hash_value, timeout, callback);
    this.tableHandler.operate(gpid, op);
};

/**
 * Constructor for helper class for calculating hash value
 * @constructor
 */
function PegasusHash() {
    this.crc64_table = [
        '0x0000000000000000', '0x7f6ef0c830358979', '0xfedde190606b12f2', '0x81b31158505e9b8b',
        '0xc962e5739841b68f', '0xb60c15bba8743ff6', '0x37bf04e3f82aa47d', '0x48d1f42bc81f2d04',
        '0xa61cecb46814fe75', '0xd9721c7c5821770c', '0x58c10d24087fec87', '0x27affdec384a65fe',
        '0x6f7e09c7f05548fa', '0x1010f90fc060c183', '0x91a3e857903e5a08', '0xeecd189fa00bd371',
        '0x78e0ff3b88be6f81', '0x078e0ff3b88be6f8', '0x863d1eabe8d57d73', '0xf953ee63d8e0f40a',
        '0xb1821a4810ffd90e', '0xceecea8020ca5077', '0x4f5ffbd87094cbfc', '0x30310b1040a14285',
        '0xdefc138fe0aa91f4', '0xa192e347d09f188d', '0x2021f21f80c18306', '0x5f4f02d7b0f40a7f',
        '0x179ef6fc78eb277b', '0x68f0063448deae02', '0xe943176c18803589', '0x962de7a428b5bcf0',
        '0xf1c1fe77117cdf02', '0x8eaf0ebf2149567b', '0x0f1c1fe77117cdf0', '0x7072ef2f41224489',
        '0x38a31b04893d698d', '0x47cdebccb908e0f4', '0xc67efa94e9567b7f', '0xb9100a5cd963f206',
        '0x57dd12c379682177', '0x28b3e20b495da80e', '0xa900f35319033385', '0xd66e039b2936bafc',
        '0x9ebff7b0e12997f8', '0xe1d10778d11c1e81', '0x606216208142850a', '0x1f0ce6e8b1770c73',
        '0x8921014c99c2b083', '0xf64ff184a9f739fa', '0x77fce0dcf9a9a271', '0x08921014c99c2b08',
        '0x4043e43f0183060c', '0x3f2d14f731b68f75', '0xbe9e05af61e814fe', '0xc1f0f56751dd9d87',
        '0x2f3dedf8f1d64ef6', '0x50531d30c1e3c78f', '0xd1e00c6891bd5c04', '0xae8efca0a188d57d',
        '0xe65f088b6997f879', '0x9931f84359a27100', '0x1882e91b09fcea8b', '0x67ec19d339c963f2',
        '0xd75adabd7a6e2d6f', '0xa8342a754a5ba416', '0x29873b2d1a053f9d', '0x56e9cbe52a30b6e4',
        '0x1e383fcee22f9be0', '0x6156cf06d21a1299', '0xe0e5de5e82448912', '0x9f8b2e96b271006b',
        '0x71463609127ad31a', '0x0e28c6c1224f5a63', '0x8f9bd7997211c1e8', '0xf0f5275142244891',
        '0xb824d37a8a3b6595', '0xc74a23b2ba0eecec', '0x46f932eaea507767', '0x3997c222da65fe1e',
        '0xafba2586f2d042ee', '0xd0d4d54ec2e5cb97', '0x5167c41692bb501c', '0x2e0934dea28ed965',
        '0x66d8c0f56a91f461', '0x19b6303d5aa47d18', '0x980521650afae693', '0xe76bd1ad3acf6fea',
        '0x09a6c9329ac4bc9b', '0x76c839faaaf135e2', '0xf77b28a2faafae69', '0x8815d86aca9a2710',
        '0xc0c42c4102850a14', '0xbfaadc8932b0836d', '0x3e19cdd162ee18e6', '0x41773d1952db919f',
        '0x269b24ca6b12f26d', '0x59f5d4025b277b14', '0xd846c55a0b79e09f', '0xa72835923b4c69e6',
        '0xeff9c1b9f35344e2', '0x90973171c366cd9b', '0x1124202993385610', '0x6e4ad0e1a30ddf69',
        '0x8087c87e03060c18', '0xffe938b633338561', '0x7e5a29ee636d1eea', '0x0134d92653589793',
        '0x49e52d0d9b47ba97', '0x368bddc5ab7233ee', '0xb738cc9dfb2ca865', '0xc8563c55cb19211c',
        '0x5e7bdbf1e3ac9dec', '0x21152b39d3991495', '0xa0a63a6183c78f1e', '0xdfc8caa9b3f20667',
        '0x97193e827bed2b63', '0xe877ce4a4bd8a21a', '0x69c4df121b863991', '0x16aa2fda2bb3b0e8',
        '0xf86737458bb86399', '0x8709c78dbb8deae0', '0x06bad6d5ebd3716b', '0x79d4261ddbe6f812',
        '0x3105d23613f9d516', '0x4e6b22fe23cc5c6f', '0xcfd833a67392c7e4', '0xb0b6c36e43a74e9d',
        '0x9a6c9329ac4bc9b5', '0xe50263e19c7e40cc', '0x64b172b9cc20db47', '0x1bdf8271fc15523e',
        '0x530e765a340a7f3a', '0x2c608692043ff643', '0xadd397ca54616dc8', '0xd2bd67026454e4b1',
        '0x3c707f9dc45f37c0', '0x431e8f55f46abeb9', '0xc2ad9e0da4342532', '0xbdc36ec59401ac4b',
        '0xf5129aee5c1e814f', '0x8a7c6a266c2b0836', '0x0bcf7b7e3c7593bd', '0x74a18bb60c401ac4',
        '0xe28c6c1224f5a634', '0x9de29cda14c02f4d', '0x1c518d82449eb4c6', '0x633f7d4a74ab3dbf',
        '0x2bee8961bcb410bb', '0x548079a98c8199c2', '0xd53368f1dcdf0249', '0xaa5d9839ecea8b30',
        '0x449080a64ce15841', '0x3bfe706e7cd4d138', '0xba4d61362c8a4ab3', '0xc52391fe1cbfc3ca',
        '0x8df265d5d4a0eece', '0xf29c951de49567b7', '0x732f8445b4cbfc3c', '0x0c41748d84fe7545',
        '0x6bad6d5ebd3716b7', '0x14c39d968d029fce', '0x95708ccedd5c0445', '0xea1e7c06ed698d3c',
        '0xa2cf882d2576a038', '0xdda178e515432941', '0x5c1269bd451db2ca', '0x237c997575283bb3',
        '0xcdb181ead523e8c2', '0xb2df7122e51661bb', '0x336c607ab548fa30', '0x4c0290b2857d7349',
        '0x04d364994d625e4d', '0x7bbd94517d57d734', '0xfa0e85092d094cbf', '0x856075c11d3cc5c6',
        '0x134d926535897936', '0x6c2362ad05bcf04f', '0xed9073f555e26bc4', '0x92fe833d65d7e2bd',
        '0xda2f7716adc8cfb9', '0xa54187de9dfd46c0', '0x24f29686cda3dd4b', '0x5b9c664efd965432',
        '0xb5517ed15d9d8743', '0xca3f8e196da80e3a', '0x4b8c9f413df695b1', '0x34e26f890dc31cc8',
        '0x7c339ba2c5dc31cc', '0x035d6b6af5e9b8b5', '0x82ee7a32a5b7233e', '0xfd808afa9582aa47',
        '0x4d364994d625e4da', '0x3258b95ce6106da3', '0xb3eba804b64ef628', '0xcc8558cc867b7f51',
        '0x8454ace74e645255', '0xfb3a5c2f7e51db2c', '0x7a894d772e0f40a7', '0x05e7bdbf1e3ac9de',
        '0xeb2aa520be311aaf', '0x944455e88e0493d6', '0x15f744b0de5a085d', '0x6a99b478ee6f8124',
        '0x224840532670ac20', '0x5d26b09b16452559', '0xdc95a1c3461bbed2', '0xa3fb510b762e37ab',
        '0x35d6b6af5e9b8b5b', '0x4ab846676eae0222', '0xcb0b573f3ef099a9', '0xb465a7f70ec510d0',
        '0xfcb453dcc6da3dd4', '0x83daa314f6efb4ad', '0x0269b24ca6b12f26', '0x7d0742849684a65f',
        '0x93ca5a1b368f752e', '0xeca4aad306bafc57', '0x6d17bb8b56e467dc', '0x12794b4366d1eea5',
        '0x5aa8bf68aecec3a1', '0x25c64fa09efb4ad8', '0xa4755ef8cea5d153', '0xdb1bae30fe90582a',
        '0xbcf7b7e3c7593bd8', '0xc399472bf76cb2a1', '0x422a5673a732292a', '0x3d44a6bb9707a053',
        '0x759552905f188d57', '0x0afba2586f2d042e', '0x8b48b3003f739fa5', '0xf42643c80f4616dc',
        '0x1aeb5b57af4dc5ad', '0x6585ab9f9f784cd4', '0xe436bac7cf26d75f', '0x9b584a0fff135e26',
        '0xd389be24370c7322', '0xace74eec0739fa5b', '0x2d545fb4576761d0', '0x523aaf7c6752e8a9',
        '0xc41748d84fe75459', '0xbb79b8107fd2dd20', '0x3acaa9482f8c46ab', '0x45a459801fb9cfd2',
        '0x0d75adabd7a6e2d6', '0x721b5d63e7936baf', '0xf3a84c3bb7cdf024', '0x8cc6bcf387f8795d',
        '0x620ba46c27f3aa2c', '0x1d6554a417c62355', '0x9cd645fc4798b8de', '0xe3b8b53477ad31a7',
        '0xab69411fbfb21ca3', '0xd407b1d78f8795da', '0x55b4a08fdfd90e51', '0x2ada5047efec8728'];
}

/**
 * Calculate crc64
 * @param   {Buffer}    buf
 * @param   {Number}    offset
 * @param   {Number}    len
 * @return  {Long}      crc64
 */
PegasusHash.prototype.crc64 = function (buf, offset, len) {
    let crc = new Long(-1, -1);
    let end = offset + len;

    let i;
    for (i = offset; i < end; ++i) {
        let index = (buf[i] ^ crc.getLowBits()) & 0xFF;
        let other = this.toLong(index);
        crc = crc.shiftRightUnsigned(8).xor(other);
    }
    return crc.not();
};

/**
 * Convert crc64_table[index] from String to Long
 * @param   {Number}    index
 * @return  {Long}      result
 */
PegasusHash.prototype.toLong = function (index) {
    let value = this.crc64_table[index];
    let high = value.slice(2, 10);
    let low = value.slice(10);
    return new Long(parseInt(low, 16), parseInt(high, 16));
};

/**
 * Calculate hash value by blob_key
 * @param   {Buffer}    blob_key
 * @return  {Long}      hash
 */
PegasusHash.prototype.hash = function (blob_key) {
    if (!blob_key || blob_key.length < 2) {
        log.error('blob_key is invalid');
        throw new Exception.InvalidParamException('blob_key is invalid');
    }

    let hashLen = blob_key.readInt16BE(0);
    if (hashLen === 0xFFFF || (hashLen + 2) > blob_key.length) {
        log.error('blob_key hash key length is invalid');
        throw new Exception.InvalidParamException('blob_key is invalid');
    }

    if (hashLen === 0) {
        return this.crc64(blob_key, 2, blob_key.length - 2);
    } else {
        return this.crc64(blob_key, 2, hashLen);
    }
};

/**
 * Calculate hash only by hashKey
 * @param   {Buffer}    hashKey
 * @return  {Long}      crc64
 */
PegasusHash.prototype.default_hash = function (hashKey) {
    if (!Buffer.isBuffer(hashKey)) {
        hashKey = new Buffer(hashKey);
    }
    return this.crc64(hashKey, 0, hashKey.length);
};

module.exports = {
    TableHandler: TableHandler,
    TableInfo: TableInfo,
};

