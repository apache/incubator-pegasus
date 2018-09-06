Notices For Client Library Development
==========

Currently C++ and Java are supported to visit Pegasus cluster. You may want to refer to [C++ documentations](https://github.com/XiaoMi/pegasus/wiki/Cpp%E5%AE%A2%E6%88%B7%E7%AB%AF%E6%96%87%E6%A1%A3) or [Java documentation](https://github.com/XiaoMi/pegasus/wiki/Java%E5%AE%A2%E6%88%B7%E7%AB%AF%E6%96%87%E6%A1%A3) for details.

Here we give some notices on the client library, which may be helpful to development of other language bindings.

## message protocol

### request to server

Message format sent to some single server of Pegasus Cluster:

`
(A special header of 48 bytes) + (body)
`

#### header

The 48-byte header consists of:

| bytes | type | comments
|-------| -----|--------- |
| 0~3 | "THFT" | (header_type)
| 4~7 | 32bit int | header version
| 8~11 | 32bit int | header_length
| 12~15 | 32bit int | header_crc32
| 16~19 | 32bit int | body_length
| 20~23 | 32bit int | body_crc32
| 24~27 | 32bit int | app_id (rDSN related concept)
| 28~31 | 32bit int | partition_index (rDSN related concept)
| 32~35 | 32bit int | client_timeout
| 36~39 | 32bit int | thread_hash (rDSN related concept)
| 40~47 | 64bit long | partition_hash (rDSN related concept)

Some notes on the above "header":
 * all ints/long in the header are in network order.
 * if send request to meta server, the app\_id & partition\_index should set to 0; if request is to replica server, the fields should be set to the target replica.
 * pegasus server may use client_timeout to do some optimization. Say, a server may simply discard a request if it has expired when server receives it
 * thread\_hash = app\_id * 7919 + partition\_index, it is used for server to decide in which thread to queue the request.
 * partition_hash should set to 0, it's only a useful field for RPC client of rDSN framework.

#### body
the body is a standard thrift struct in binary protocol:

TMessageBegin + args + TMessageEnd.

You should write a thrift "TMessage" in TMessageBegin, the structure of TMessage is:
* name: the RPC name (please refer to Java client for detail)
* type: TMessage.CALL
* seqid: the seqid int

### response from server

response got from pegasus server:

`
total_response_length(4 bytes) +  error_code_thrift_struct + response_body
`

Some notes on the above response:

* **error_code_thrift_struct** is an error_code struct in thrift binary protocol, usually it indicates some error of service status than the response of some specific rpc call. For example, for a meta server, this error may indicate "the meta server is not leader"; for a read/write request to replica server, this error may indicate that the replica server is not a primary or don't serve the partition"
* **response_body** is a standard thrift rpc response of the rpc call, with the format as follows:
  * TMessageBegin: rpc_name, TMessage.T_REPLY, seqid_integer
  * response_args
  * TMessageEnd

## write/read request process

You can refer to [TableHandler.java](https://github.com/XiaoMi/pegasus-java-client/blob/thrift-0.11.0-inlined/src/main/java/com/xiaomi/infra/pegasus/rpc/async/TableHandler.java) for the detailed RPC process in write/read request RPCs.

## how to generate code in thrift

There are [3 IDL files](https://github.com/XiaoMi/pegasus-java-client/tree/thrift-0.11.0-inlined/idl) for RPC client:
* base.thrift: a placeholder for rDSN specific structures(blob, error\_code, task\_code, RPC_address, gpid), you may use thrift to generate a sketch, and implement the details all by yourself.
* replication.thrift: messages and RPCs used for communicate with meta server. Using generated code is ok.
* rrdb.thrift: messages and RPCs used for communicate with replica server. Using generated code is ok.

Due to some history reasons, RPC names defined in the IDL can't be recognized by server right now. A proper name should be set manually, please refer to [operators](https://github.com/XiaoMi/pegasus-java-client/tree/thrift-0.11.0-inlined/src/main/java/com/xiaomi/infra/pegasus/operator) for details. Besides, you may also need to refer
to [base](https://github.com/XiaoMi/pegasus-java-client/tree/thrift-0.11.0-inlined/src/main/java/com/xiaomi/infra/pegasus/base) for how to implement serialization for rDSN specific structures.
