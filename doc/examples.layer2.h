/*!
 @defgroup example-layer2 Example
 @ingroup dev-layer2

Build a scalable and reliable counter service with built-in replication support

We will continue using the
[counter](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service)
example and turn it from a single node service to a partitioned and replicated service for
scalability and reliability. A more serious case is [RocksDB](https://github.com/imzhenyu/rocksdb).

Many people asked about what kinds of replication protocol we're using. Currently, our default
provider largely follows [PacificA](http://research.microsoft.com/apps/pubs/default.aspx?id=66814).
We may have other protocols as framework providers in layer 2 in the future.

### STEP 1. Code generation

Code generation is very similar to what we do for the single-node service development before, except
for the following two minor additions.

##### STEP 1.1 Annotate the interface, tell rDSN which methods need to be replicated.

Methods for a service usually contain both read-only and write methods. Replication is necessary for
only those write methods. Developers therefore use a separated annotation file (in order to support
both Thrift and Google Protoc, as well as further ones) to mark them. Following is the example as in
[here](https://github.com/Microsoft/rDSN/blob/master/tutorial/counter.thrift.annotations).

```bash
; annotation format
;[type.name[[.subname]...]]
;key = vlaue

[service.counter]
stateful = true ; counter is a stateful service

[function.counter.add]
write = true  ; counter.add is a write function

[function.counter.read]
write = false
```
##### STEP 1.2 Generate the code with both the IDL definition file and the annotation file as
inputs.

This is exactly the same compared to [step 1.2 for single-node
development](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-12),
except that we add the **replication** argument for the code generation tool.

```bash
~/projects/rdsn/tutorial$ dsn.cg.sh counter.thrift cpp counter.rep replication
generate 'counter.rep/CMakeLists.txt' successfully!
generate 'counter.rep/counter.app.example.h' successfully!
generate 'counter.rep/counter.client.h' successfully!
generate 'counter.rep/counter.code.definition.h' successfully!
generate 'counter.rep/config.ini' successfully!
generate 'counter.rep/counter.main.cpp' successfully!
generate 'counter.rep/counter.server.h' successfully!
generate 'counter.rep/counter.types.h' successfully!
...
```
### STEP 2. Implement application logic

This remains exactly the
[same](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-2-implement-application-logic).

### STEP 3. Implement application specific replication logic

This is the extra step we need for replication, mostly about writing helper functions for
replication so that it can work smoothly and efficiently for the given application. Description
about the helper functions can be found
[here](https://github.com/Microsoft/rDSN/blob/master/include/dsn/dist/replication/replication_app_base.h#L50),
and the implementation for our counter service example is
[here](https://github.com/Microsoft/rDSN/tree/master/tutorial/counter.replication).

### STEP 4. Register the implementation, compile and run

We register the new service implementation to rDSN in main (done by code generation), and compile
should be ok.

```C++
// register replication application provider
dsn::replication::register_replica_provider< ::dsn::example::counter_service_impl>("counter");
```

Note on windows, we still need to specify where are the boost header files and libraries, etc.

```bash
c:\Projects\rDSN\tutorial\counter.rep\build>cmake .. -G "Visual Studio 12 2013"
-DBOOST_INCLUDEDIR=c:\local\boost_1_57_0 -DBOOST_LIBRARYDIR=c:\local\boost_1_57_0\lib32-msvc-12.0
```

Before running the system, we need to configure the meta server and the replica servers.

```bash
[apps.client]
name = client
type = client
arguments = counter.instance0
run = true
count = 2

[replication.app]
app_name = counter.instance0
app_type = counter
partition_count = 1
max_replica_count = 3
```

Then go to the directory where 'counter.exe' is generated and run; make sure 'config.ini' is also
copied there.

```bash
00:00:35.045(35045) replica3.replication0.0000000003c93bc8: 1.0 @ localhost:34803: mutation 14.39
ack_prepare_message
00:00:35.057(35057) replica2.replication0.0000000003c93ec8: 1.0 @ localhost:34802: mutation 14.39
on_append_log_completed, err = 0
00:00:35.057(35057) replica2.replication0.0000000003c93ec8: 1.0 @ localhost:34802: mutation 14.39
ack_prepare_message
00:00:35.078(35078) replica1.replication0.0000000003c932c8: 1.0 @ localhost:34801: mutation 14.39
on_prepare_reply from localhost:34803
00:00:35.091(35091) replica1.replication0.0000000003c93088: 1.0 @ localhost:34801: mutation 14.39
on_prepare_reply from localhost:34802
00:00:35.091(35091) replica1.replication0.0000000003c93088: TwoPhaseCommit, 1.0 @ localhost:34801:
mutation 14.39 committed, err = 0
call RPC_COUNTER_COUNTER_ADD end, return ERR_SUCCESS
call RPC_COUNTER_COUNTER_READ end, return ERR_SUCCESS
00:00:35.624(35624) replica1.replication0.0000000003c8c1a0: 1.0 @ localhost:34801: mutation 14.40
init_prepare
00:00:35.624(35624) replica1.replication0.0000000003c8c1a0: 1.0 @ localhost:34801: mutation 14.40
send_prepare_message to localhost:34803 as PS_SECONDARY
00:00:35.624(35624) replica1.replication0.0000000003c8c1a0: 1.0 @ localhost:34801: mutation 14.40
send_prepare_message to localhost:34802 as PS_SECONDARY
00:00:35.676(35676) replica3.replication0.0000000003c93fa8: 1.0 @ localhost:34803: mutation 14.40
on_prepare
00:00:35.676(35676) replica3.replication0.0000000003c93fa8: TwoPhaseCommit, 1.0 @ localhost:34803:
mutation 14.39 committed, err = 0
00:00:35.705(35705) replica2.replication0.0000000000eae958: 1.0 @ localhost:34802: mutation 14.40
on_prepare
00:00:35.705(35705) replica2.replication0.0000000000eae958: TwoPhaseCommit, 1.0 @ localhost:34802:
mutation 14.39 committed, err = 0
00:00:35.724(35724) replica1.replication0.0000000003c93988: 1.0 @ localhost:34801: mutation 14.40
on_append_log_completed, err = 0
00:00:35.776(35776) replica3.replication0.0000000003c93d48: 1.0 @ localhost:34803: mutation 14.40
on_append_log_completed, err = 0
00:00:35.776(35776) replica3.replication0.0000000003c93d48: 1.0 @ localhost:34803: mutation 14.40
ack_prepare_message
```

### STEP 5. Use test/vtest to batch test the program

It's natural that we sometimes want to test codes by running multiple programs simultaneously. We
provide a simple script for occasions like that. In the "tutorial/counter.test/" folder we prepared
an example. "test" is for simulator mode and "vtest" is for nativerun mode. They're basically the
same thing with different config files.

To use it, copy the .cmd and .ini files into the folder containing .exe file. Remember to set the
iteration times and program name in the .cmd. And configure the .ini file if necessary. Then run the
.cmd files.

Normally you should see multiple programs bumping out running at the same time.



 */
