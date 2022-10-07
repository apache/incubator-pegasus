<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# pegasus-nodejs-client

## Installation
`npm install pegasus-nodejs-client --save`


## Usage
### Create pegasus client:  `create(configs)`
```javascript
let pegasusClient = require('pegasus-nodejs-client');
let configs = {
    'metaServers'      : [     // required - meta server address array
        '127.0.0.1:34601',
        '127.0.0.1:34602',
        '127.0.0.1:34603',
     ],
    'operationTimeout' : 5000, // optional - operation timeout in milliseconds - default value: 1000ms
    'log' : log,               // optional - log4js logger - details in log section
};
let client = pegasusClient.create(configs);

```

### Close pegasus client:  `close()`
```javascript
// we strongly recommend that you should close client when you finish your operations
client.close();
```


### Get value: `get(tableName, args, callback)`
```javascript
let getArgs = {
    'hashKey' : new Buffer('1'),    // required (Buffer)
    'sortKey' : new Buffer('1'),    // required (Buffer)
    'timeout' : 2000,               // optional - timeout in milliseconds - default: operationTimeout set in client config
};
client.get('temp', getArgs, function(err, result){
    // err will be null, result will be hashKey-sortKey-value object when succeed
    // result.hashKey is hashKey, result.sortKey is sortKey, result.value is value
    // otherwise, result will be null, err will be instanceof PException
});

```
> Notice:  
> If pegasus can't get value according to hashKey and sortKey in args, client DO NOT consider it as an error.  
> ```javascript
>  let getArgs = {
>      'hashKey' : new Buffer('not-exist'),
>      'sortKey' : new Buffer('not-found'),
>  };
>  client.get(tableName, getArgs, function(err, result){
>      assert.equal(null, err);
>      assert.deepEqual(new Buffer(''), result);
>  });
> ``` 

### Set value: `set(tableName, args, callback)`
```javascript
let setArgs = {
    'hashKey' : new Buffer('1'),  // required (Buffer)
    'sortKey' : new Buffer('1'),  // required (Buffer)
    'value'   : new Buffer('1'),  // required (Buffer)
    'ttl'     : 86400,            // optional - time to live in seconds - default: 0
    'timeout' : 2000,             // optional - timeout in milliseconds - default: operationTimeout set in client config
};
client.set(tableName, setArgs, function(err){
    // err will be null when succeed, otherwise instanceof PException
});
```
> Notice: ttl is different with timeout  
> For example, if you set ttl is 86400 seconds, it means the value you set will be expired after one day.   
> Default ttl is 0, which means the value will not expire automatically.   

### Delete value:  `del(tableName, args, callback)`
```javascript
let delArgs = {
    'hashKey' : new Buffer('1'),  // required (Buffer)
    'sortKey' : new Buffer('1'),  // required (Buffer)
    'timeout' : 2000,             // optional - timeout in milliseconds - default: operationTimeout set in client config
};
client.del(tableName, delArgs, function(err){
    // err will be null when succeed, otherwise instanceof PException
});
```

### MultiGet: `MultiGet(tableName, args, callback)`
```javascript
let multiGetArgs = {
    'hashKey'       : new Buffer('1'),  // required (Buffer)
    'sortKeyArray'  : [                 // required (Array)
        new Buffer('1'),
        new Buffer('11'),
        new Buffer('22'),
    ],
    'timeout'       : 2000,             // optional - timeout in milliseconds - default: operationTimeout set in client config
    'maxFetchCount' : 100,              // optional - default: 100
    'maxFetchSize'  : 1000000,          // optional(Byte) - default: 1000000
};
client.multiGet(tableName, multiGetArgs, function(err, result){
    // err will be null, result will be hashKey-sortKey-value object array when all operations succeed
    // result[i].hashKey is hashKey, result[i].sortKey is sortKey, result[i].value is value
});
```
> Notice: multiGet is used for getting values under same hashKey, params and result in multiGetArgs:
> - sortKeyArray: if it is an empty array, it means get all sortKeys under the hashKey
> - maxFetchCount: max count of k-v pairs, if maxFetchCount <= 0 means no limit, default value is 100
> - maxFetchSize: max size of k-v pairs, if maxFetchSize <= 0 means no limit, default value is 1000000

### BatchGet: `batchGet(tableName, argsArray, callback)`
```javascript
let batchGetArgArray = [];
batchGetArgArray[0] = {
    'hashKey' : new Buffer('1'),    // required (Buffer)
    'sortKey' : new Buffer('11'),   // required (Buffer)
    'timeout' : 2000,               // optional - timeout in milliseconds - default: operationTimeout set in client config
};
batchGetArgArray[1] = {
    'hashKey' : new Buffer('1'),
    'sortKey' : new Buffer('22'),
    'timeout' : 2000,
};
client.batchGet(tableName, batchGetArgArray, function(err, result){
    // err will be always be null, result is {'error': err, 'data': result} array
    // if batchGet[i] operation succeed, result[i].error will be null
    // result[i].data.hashKey is hashKey, result[i].data.sortKey is sortKey, result[i].data.value is value
    // else result[i].error will be instance of PException, result[i].data will be nul
});
```
> Notice: batchGet is not atomic operation  
> batchGet is different from multiGet, you can get values under several hashKeys.  

### multiSet: `multiSet(tableName, args, callback)`
```javascript
let array = [];
array[0] = {
    'key'   : new Buffer('11'),     // required - sortKey (Buffer)
    'value' : new Buffer('111'),    // required (Buffer)
};
array[1] = {
    'key'   : new Buffer('22'),
    'value' : new Buffer('222'),
};

let args = {
    'hashKey'           : new Buffer('1'),  // required (Buffer)
    'sortKeyValueArray' : array,            // required (Array)
    'ttl'               : 86400,            // optional - time to live in seconds - default: 0
    'timeout'           : 2000,             // optional - timeout in milliseconds - default: operationTimeout set in client config
};
client.multiSet(tableName, args, function(err){
    // err will be null when succeed, otherwise instanceof PException
});
```

### batchSet: `batchSet(tableName, argsArray, callback)`
```javascript
let argArray = [];
argArray[0] = {
    'hashKey' : new Buffer('1'),    // required (Buffer)
    'sortKey' : new Buffer('11'),   // required (Buffer)
    'value'   : new Buffer('11'),   // required (Buffer)
    'timeout' : 2000,               // optional - timeout in milliseconds - default: operationTimeout set in client config
};
argArray[1] = {
    'hashKey' : new Buffer('1'),
    'sortKey' : new Buffer('22'),
    'value'   : new Buffer('22'),
    'timeout' : 2000,
};
client.batchSet(tableName, argArray, function(err){
    // err will be always be null, result is {'error': err} array
    // if batchSet[i] operation succeed, result[i].error will be null
    // else result[i].error will be instance of PException
});
```

## Log
We use [log4js](https://github.com/log4js-node/log4js-node) as logging library.  
Default log configuration is in `log_config.js` file:  
```javascript
let filename = "./logs/"+process.pid+"/pegasus-nodejs-client.log";
let logConfig = {
   appenders: { 
     pegasus: {
       type: "file",            // logs are saved as file
       filename: filename, 
       maxLogSize: 104857600,   // max log size for each log is 100M
       backups: 10              // keep 10 log files at most
     } 
   },
   categories: {
     default: { appenders: ["pegasus"], level: "INFO" } 
   }
};
```

## Exception and error type

All errors returned from callback are instance of PException, basic exception in pegasus client.  
Each exception has an error type to indicate reason of failure, here are some common error types:
* ERR_TIMEOUT - caused by operation timeout, users can retry or lengthen timeout when creating client
* ERR_SESSION_RESET - caused by socket reset or reconnecting
* ERR_INVALID_STATE - caused by server reconfiguration
* ERR_OBJECT_NOT_FOUND - caused by wrong table name or server reconfiguration

## Test
Tests rely on pegasus onebox cluster, referring to [Using pegasus onebox](https://pegasus.apache.org/overview/onebox/)  
Before test, you should start onebox cluster.
```
npm test
```

## TODO
* [x] supplement README doc
    * [x] Exception
    * [x] Test
* [x] supplement error and exception in source code
* [ ] support other operations
* [ ] benchmark
* [x] kill test and stability test on both onebox and cluster  

