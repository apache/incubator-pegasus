# pegasus-nodejs-client
Official NodeJS client for [xiaomi/pegasus](https://github.com/XiaoMi/pegasus)

## Installation

## Usage
### Create pegasus client:  `create(configs)`
```
let pegasusClient = require('../');
let configs = {
    'metaList'   : ['127.0.0.1:34601',
                    '127.0.0.1:34602',
                    '127.0.0.1:34603'], // required - meta server address array
    'rpcTimeOut' : 5000,                // optional - operation timeout in millisecond - default value: 1000ms
};
let client = pegasusClient.create(configs);

```

### Close pegasus client:  `close()`
```
// we strongly recommend that you should close client when you finish your operations
client.close();
```


### Get value: `get(tableName, args, callback)`
```
let getArgs = {
    'hashKey' : '1',    // required (String or Buffer)
    'sortKey' : '1',    // required (String or Buffer)
    'timeout' : 2000,   // optional - operation timeout in millisecond - default: timeout set in client config
};
client.get('temp', getArgs, function(err, result){
    // err will be null, result will be value when succeed
    // otherwise, result will be null, err will be instanceof PException
});

```
> Notice:<br/>
> If pegasus can't get value according to hashKey and sortKey in args, client DO NOT consider it as an error.<br/>
> ```
> client.get(tableName, {'hashKey':'not-exist','sortKey':'not-found',}, function(err, result){
>      assert(null, err);
>      assert('', result);
>  });
> ``` 

### Set value: `set(tableName, args, callback)`
```
let setArgs = {
    'hashKey' : '1',  // required (String or Buffer)
    'sortKey' : '1',  // required (String or Buffer)
    'value'   : '1',  // required (String or Buffer)
    'ttl'     : 100,  // optional - value lifetime in second - default: 0
    'timeout' : 2000, // optional - operation timeout in millisecond - default: timeout set in client config
};
client.set(tableName, setArgs, function(err){
    // err will be null when succeed, otherwise instanceof PException
});
```
> Notice: ttl is different with timeout<br/>
> For example, if you set ttl is 10 seconds, it means the value you set will be expired after 10 seconds. Default ttl is 0, which means the value will not expire automatically.<br/> 

### Delete value:  `del(tableName, args, callback)`
```
let delArgs = {
    'hashKey' : '1',  // required (String or Buffer)
    'sortKey' : '1',  // required (String or Buffer)
    'timeout' : 2000, // optional - operation timeout in millisecond - default: timeout set in client config
};
client.del(tableName, delArgs, function(err){
    // err will be null when succeed, otherwise instanceof PException
});
```

### MultiGet: `MultiGet(tableName, args, callback)`
```
let multiGetArgs = {
    'hashKey'       : '1',               // required (String or Buffer)
    'sortKeyArray'  : ['1', '11', '22'], // required (Array)
    'timeout'       : 2000,              // optional - operation timeout in millisecond - default: timeout set in client config
    'max_kv_count'  : 100,               // optional - default: 100
    'max_kv_size'   : 1000000,           // optional(Byte) - default: 1000000
};
client.multiGet(tableName, multiGetArgs, function(err, result){
    // err will be null, result will be value array when all operations succeed
    // result[i].key.data is sortKey, result[i].value.data is value
});
```
> Notice: multiGet is used for getting values under same hashKey, params and result in multiGetArgs:
> - sortKeyArray: if it is an empty array, it means get all sortKeys under the hashKey
> - max_kv_count: max count of k-v pairs, if max_kv_count <= 0 means no limit, default value is 100
> - max_kv_size: max size of k-v pairs, if max_kv_size <= 0 means no limit, default value is 1000000

### BatchGet: `batchGet(tableName, argsArray, callback)`
```
let batchGetArgArray = [];
batchGetArgArray[0] = {
    'hashKey' : '1',    // required (String or Buffer)
    'sortKey' : '11',   // required (String or Buffer)
    'timeout' : 2000,   // optional - operation timeout in millisecond - default: timeout set in client config
};
batchGetArgArray[1] = {
    'hashKey' : '1',
    'sortKey' : '22',
    'timeout' : 2000,
};
client.batchGet(tableName, batchGetArgArray, function(err, result){
    // err will be null, result will be value array when all operations succeed
    // result[i].hashKey is hashKey, result[i].sortKey is sortKey, result[i].value is value
    // batchGet is not atomic, if any get operation failed, batchGet will stop
});
```
> Notice: batchGet is not atomic operation <br/>
> batchGet is different with multiGet, you can get values under several hashKeys.<br/>
> However, this operation will be stopped when any one of single get failed

### multiSet: `multiSet(tableName, args, callback)`
```
let array = [];
array[0] = {
    'key'   : '11',     // required - sortKey (String or Buffer)
    'value' : '111',    // required (String or Buffer)
};
array[1] = {
    'key'   : '22',
    'value' : '222',
};

let args = {
    'hashKey'           : '1',      // required (String or Buffer)
    'sortKeyValueArray' : array,    // required (Array)
    'ttl'               : 0,        // optional - value lifetime in second - default: 0
    'timeout'           : 2000,     // optional - operation timeout in millisecond - default: timeout set in client config
};
client.multiSet(tableName, args, function(err){
    // err will be null when succeed, otherwise instanceof PException
});
```

### batchSet: `batchGet(tableName, argsArray, callback)`
```
let argArray = [];
argArray[0] = {
    'hashKey' : '1',    // required (String or Buffer)
    'sortKey' : '11',   // required (String or Buffer)
    'value'   : '11',   // required (String or Buffer)
    'timeout' : 2000,   // optional - operation timeout in millisecond - default: timeout set in client config
};
argArray[1] = {
    'hashKey' : '1',
    'sortKey' : '22',
    'value'   : '22',
    'timeout' : 2000,
};
client.batchSet(tableName, argArray, function(err){
    // err will be null when all operations succeed
    // batchSet is not atomic, if any get operation failed, batchSet will stop
});
```

## Exception
All errors callback returns are instance of PException, basic exception in pegasus client.


## Test
Tests rely on pegasus onebox cluster, referring to [Using pegasus onebox](https://github.com/XiaoMi/pegasus/wiki/%E4%BD%93%E9%AA%8Conebox%E9%9B%86%E7%BE%A4)
<br />Before test, you should start onebox cluster.


## TODO
* [ ] supplement README doc
* [ ] support other operations
* [ ] add more unit tests and mock tests
* [ ] benchmark



