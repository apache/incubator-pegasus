/*!
 @defgroup example-layer1 Example
 @ingroup dev-layer1

  Quickly build a counter service with built-in tools

 @{


 We will develop & operate a counter service here and go through many of rDSN's features at layer 1.
Before exercising this tutorial, please make sure you have already
[installed](https://github.com/Microsoft/rDSN/wiki/Installation) rDSN on your machine.

### Quick links

* STEP 1. [Write the service interface and
run!](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-1-write-the-service-interface-and-run)
* STEP 2. [Implement application
logic](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-2-implement-application-logic)
* STEP 3. [Deploy with single-executable, multiple-role,
multiple-instance](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-3-deploy-with-single-executable-multiple-role-multiple-instance)
* STEP 4. [Use Toollet - understand what is happening with
logging](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-4-use-toollet---understand-what-is-happening-with-logging)
* STEP 5. [View ALL state in a single debugger, and debug w/o worring about false
timeouts](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-5-view-all-state-in-a-single-debugger-and-debug-wo-worring-about-false-timeouts)
* STEP 6. [Deterministic execution for easy
debugging](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-6-deterministic-execution-for-easy-debugging)
* STEP 7. [Scale-up the
service](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-7-scale-up-the-service)
* STEP 8. [Systematic test against various failures and scheduling
decisions](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-8-systematic-test-against-various-failures-and-scheduling-decisions)
* STEP 9. [Understanding the performance using the profiler
tool](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-9-understanding-the-performance-using-the-profiler-tool)
* STEP 10. [Operate your system with local and remote cli
](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-10-operate-your-system-with-local-and-remote-cli)
* STEP 11. Check global correctness using global assertion across nodes
* STEP 12. Handle system overloading using admission controller
* STEP 13. [Plug-in your own low level components
(optional)](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-13-plug-in-your-own-low-level-components-optional)
* STEP 14. [Connect the service with other languages
(optional)](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-14-connect-the-service-with-other-languages-optional)
* STEP 15. [Open service with multiple ports
(optional)](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-15-open-service-with-multiple-ports-optional)
* STEP 16. [Find more information about the config.ini
(optional)](https://github.com/Microsoft/rDSN/wiki/Find-useful-information-of-config.ini)

### STEP 1. Write the service interface and run!

##### STEP 1.1
Use the interface definition language from [Apache Thrift](https://thrift.apache.org/docs/idl) or
[Google Protocol Buffers](https://developers.google.com/protocol-buffers/docs/proto) to describe
your service interface. In this case, we are using thrift and writing the following interface
definition for our service into counter.thrift.
```C++
namespace cpp dsn.example

struct count_op
{
   1: string name;
   2: i32    operand;
}

service counter
{
    i32 add(1:count_op op);
    i32 read(1:string name);
}
```

##### STEP 1.2

Use our tool ```dsn.cg.sh``` or ```dsn.cg.bat``` from the **installation** directory to generate
code from the above file. Note we need to have the [enhanced Apache
Thrift](https://github.com/imzhenyu/thrift/tree/master/pre-built) or [Google
Protoc](https://github.com/imzhenyu/protobuf/tree/master/pre-built) tool in the sub-directory
%os-name%(e.g., [Linux](https://github.com/Microsoft/rDSN/tree/master/bin/Linux)) of where
```dsn.cg``` is installed (done by default now).

```
~/projects/rdsn/tutorial$ dsn.cg.sh counter.thrift cpp counter single
generate 'counter/CMakeLists.txt' successfully!
generate 'counter/counter.app.example.h' successfully!
generate 'counter/counter.client.h' successfully!
generate 'counter/counter.code.definition.h' successfully!
generate 'counter/config.ini' successfully!
generate 'counter/counter.main.cpp' successfully!
generate 'counter/counter.server.h' successfully!
generate 'counter/counter.types.h' successfully!
...
```
An introduction about these files.

* counter.main.cpp - the main program, which defines the possible service app and tool **roles** in
the final executable. In this case, the counter server & client service, as well as all the default
tools are registered.
* config.ini - configuration file specifying what are the **role instances** for services and tools
for a particular running process.
* counter.code.definition.h - defines the event/task code for the RPC calls.
* counter.client/server.h - define the server and client classes for counter service.
* counter.app.example.h - an example wrapper about how to put single/multiple client/server into
service apps.
* counter.types.h - how to marshall and unmarshall the request/response messages across network
using rDSN's internal binary encoding/decoding. You may also use the ones from Thrift (more).
* counter.check.h/.cpp - sketch for writing cross-nodes global assertions.
* CMakeLists.txt - file for cmake.

##### STEP 1.3

Build and run

```
~/projects/rdsn/tutorial/counter$ mkdir build
~/projects/rdsn/tutorial/counter$ cd build
~/projects/rdsn/tutorial/counter/build$ cmake ..
~/projects/rdsn/tutorial/counter/build$ make -j
~/projects/rdsn/tutorial/counter/build$ cd bin/counter
~/projects/rdsn/tutorial/counter/build/bin/counter$ ./counter config.ini
unknown: simulation.random seed for this round is -970799620
... exec RPC_COUNTER_COUNTER_READ ... (not implemented)
call RPC_COUNTER_COUNTER_READ end, return ERR_SUCCESS
... exec RPC_COUNTER_COUNTER_ADD ... (not implemented)
call RPC_COUNTER_COUNTER_ADD end, return ERR_SUCCESS
...
```

Note for the above "cmake .." on Windows, you may encounter certain errors, and you may use the
command similar to the following instead.

```
~/projects/rdsn/tutorial/counter/build$ cmake .. -DCMAKE_INSTALL_PREFIX=c:\rdsn
-DBOOST_INCLUDEDIR="c:\boost_1_57_0" -DBOOST_LIBRARYDIR="c:\boost_1_57_0\lib64-msvc-12.0" -G "NMake
Makefiles"
```

Also note for the above "make -j" on Windows, you could build in Visual Studio 2013 instead.
Remember to enter "Configuration Manager" to change "Platform" to x64 if necessary. If you meet with
problems like "value xx doesn't match value xx xx.obj", you must be confused with "Release" and
"Debug".


### STEP 2. Implement application logic

In order to plug-in the real application logic, we need to write a new service class inheriting
**counter_service** generated in counter.server.h. Note rDSN has some **[programming
rules](https://github.com/Microsoft/rDSN/wiki/Programming-Rules-and-FAQ)** to ensure the built-in
tools and frameworks can work.

```C++
 6 class counter_service_impl : public counter_service
 7 {
 8 public:
 9    virtual void on_add(const ::dsn::example::count_op& op, ::dsn::rpc_replier<int32_t>& reply)
10    {
11        auto result = (_counters[op.name] += op.operand);
12        reply(result);
13    }
14
15    virtual void on_read(const std::string& name, ::dsn::rpc_replier<int32_t>& reply)
16    {
17        auto it = _counters.find(name);
18        auto result = it != _counters.end() ? it->second : 0;
19        reply(result);
20    }
21
22 private:
23    std::map<std::string, int32_t> _counters;
24 };
```
To integrate the new class into the real execution, we need to change the member service in class
**counter_server_app** from **counter_service** to **counter_service_impl**. We also change the
logic in **counter_client_app** to do some meaningful test (all in **counter.app.example.h**).
```C++
90 ::dsn::example::count_op req = {"counter1", 1};
94 std::cout << "call RPC_COUNTER_COUNTER_ADD end, return " << resp << ", err = " << err.to_string()
<< std::endl;
...
```
After re-compiling and executing the program, we get:
```
~/projects/rdsn/tutorial/counter/build/bin/counter$ ./counter config.ini
unknown: simulation.random seed for this round is -1139212112
call RPC_COUNTER_COUNTER_ADD end, return 1, err = ERR_SUCCESS
call RPC_COUNTER_COUNTER_READ end, return 1, err = ERR_SUCCESS
call RPC_COUNTER_COUNTER_ADD end, return 2, err = ERR_SUCCESS
call RPC_COUNTER_COUNTER_READ end, return 2, err = ERR_SUCCESS
...
```
rDSN provides both synchronous and asynchronous client access for the service as in
**counter.client.h** in this case. To use asynchronous client, developers implement customized
handlers to process the response from the server, e.g., **end_add** and **end_read** in this case.
We skip here for simplicity.

### STEP 3. Deploy with single-executable, multiple-role, multiple-instance

You may already feel strange that we don't start a client and a server separately for the above
exercises. rDSN advocates a single-executable, multiple-role, multiple-instance deployment model. In
the above case, both the client and the server roles are registered in **main** of the host process,
so their execution code are within the same executable after compilation. In order to enable this,
rDSN actually has some rules like no global variables etc. to avoid confliction, see
[here](https://github.com/Microsoft/rDSN/wiki/Programming-Rules-and-FAQ) for more information.

```C++
int main(int argc, char** argv)
{
  // register all possible service apps
  dsn::service::system::register_service< ::dsn::example::counter_server_app>("counter_server");
  dsn::service::system::register_service< ::dsn::example::counter_client_app>("counter_client");

  // register all possible tools and toollets
  dsn::tools::register_tool<dsn::tools::nativerun>("nativerun");
  dsn::tools::register_tool<dsn::tools::simulator>("simulator");
  ...
```

When for the real execution, what roles and how many instances for each role as well as their
arguments are specified in the configuration file. The following configuration starts one server and
one client for the given process.

```
[apps.server]
name = server
type = counter_server
arguments =
ports = 27001
run = true

[apps.client]
name = client
type = counter_client
arguments = localhost 27001
count = 1
run = true
```

You may also notice that we also register **tools** etc. in the above **main** function. Similarly,
the configuration file specifies what tools should be started for a given run.

```
[core]
;tool = simulator
tool = nativerun
```
We may try **nativerun** in this case. Then re-run the application (no need to re-compile).

```
~/projects/rdsn/tutorial/counter/build/bin/counter$ ./counter config.ini
call RPC_COUNTER_COUNTER_ADD end, return 1, err = ERR_SUCCESS
call RPC_COUNTER_COUNTER_READ end, return 1, err = ERR_SUCCESS
call RPC_COUNTER_COUNTER_ADD end, return 2, err = ERR_SUCCESS
call RPC_COUNTER_COUNTER_READ end, return 2, err = ERR_SUCCESS
...
```

You won't see differences except that the output becomes much slower. This is because with the
native run tool, we are emitting client requests every 1 physical second, while with the simulator
tool, 1 second is much faster. Next, we can change the configuration in config.ini so the client and
the server are deployed as two different processes (possibly on two remote machines then). Or, we
can simply use the following commands to start the client and server respectively in two separated
processes. Note in this case ```[core]tool = nativerun``` must be set to ensure they can communicate
with each other.

```
~/projects/rdsn/tutorial/counter/build/bin/counter$ ./counter config.ini -app_list server
~/projects/rdsn/tutorial/counter/build/bin/counter$ ./counter config.ini -app_list client
```


### STEP 4. Use Toollet - understand what is happening with logging

Logging is still the most widely adopted mechanism to understand what happens in the system. Instead
of manually and intensively writing **printf** in the code, rDSN provides the **tracer** tool with
automatic logging. To load the tracer tool, we need to add **tracer** to the **toollets** list under
**[core]** section. A toollet is similar to a **tool** like **simulator** and **nativerun**, except
that it can co-exist with the other toollets and tools, while **tools** conflict with each other.


```
[core]
tool = simulator
;tool = nativerun
toollets = tracer
;toollets = tracer, profiler, fault_injector
pause_on_start = false

logging_factory_name = dsn::tools::screen_logger
```

With the tracer tool, here is what we can see. Note for simplicity, unless explicitly specified, we
will all use the **simulator** tool and enable both client and server in the configuration from now
on.


```
...
client.default.0: RPC_COUNTER_COUNTER_ADD RPC.CALL: localhost:1 => localhost:27001, rpc_id =
e3f394e0d2eae776, callback_task = 00000000012b0000, timeout = 5000ms
client.default.0: RPC_COUNTER_COUNTER_ADD RPC.REQUEST.ENQUEUE, task_id = 00000000012acf80,
localhost:1 => localhost:27001, rpc_id = e3f394e0d2eae776
server.default.0: RPC_COUNTER_COUNTER_ADD EXEC BEGIN, task_id = 00000000012acf80, localhost:1 =>
localhost:27001, rpc_id = e3f394e0d2eae776
server.default.0: RPC_COUNTER_COUNTER_ADD_ACK RPC.REPLY: localhost:27001 => localhost:1, rpc_id =
e3f394e0d2eae776
server.default.0: RPC_COUNTER_COUNTER_ADD_ACK RPC.RESPONSE.ENQUEUE, task_id = 00000000012b0000,
localhost:27001 => localhost:1, rpc_id = e3f394e0d2eae776
client.default.0: RPC_COUNTER_COUNTER_ADD_ACK EXEC BEGIN, task_id = 00000000012b0000,
localhost:27001 => localhost:1, rpc_id = e3f394e0d2eae776
client.default.0: RPC_COUNTER_COUNTER_ADD_ACK EXEC END, task_id = 00000000012b0000, err =
ERR_SUCCESS
server.default.0: RPC_COUNTER_COUNTER_ADD EXEC END, task_id = 00000000012acf80, err = ERR_SUCCESS
call RPC_COUNTER_COUNTER_ADD end, return 1, err = ERR_SUCCESS
...
```
Every log entry starts with a header like "client.default.0", which means it was on the node with
name 'client', in the thread pool named "default", and executed by the thread with index 0. Our
tracing tool instruments all the asynchronous points in the system, therefore in this case you can
easily see how the **RPC_COUNTER_COUNTER_ADD** request is processed in the system.

The tracing tool also comes with fine-grain configuration, shown below. We turn on tracing for all
tasks by default, and put LPC_RPC_TIMEOUT as an exception because it is an internal task used by RPC
engine in rDSN.

```
[task.default]
is_trace = true

[task.LPC_RPC_TIMEOUT]
is_trace = false
```



### STEP 5. View ALL state in a single debugger, and debug w/o worring about false timeouts

A distributed system is composed of multiple nodes. Since rDSN enables all nodes running in one
single physical node, we can  pause the whole system in a debugger, and inspect all memory state
easily. rDSN provides a special symbol called **dsn_all** (or **dsn_all** and **dsn_apps** in the
future) for tracking most of the variables in the system (e.g., all app instances, all tools, and
the engine). Following is a case showing the global state (not for this case though).

![dsn_all](https://raw.githubusercontent.com/Microsoft/rDSN/master/resources/rdsn-state-all.jpg)

Because we can use **simualtor** as our underlying tool when debugging, which virtualizes the time,
we don't need to worry about false timeout at all when pausing and inspecting the memory state in
debugger - a common annoying problem when debugging distributed systems.

### STEP 6. Deterministic execution for easy debugging

Implementing the application logic usually requires many rounds of try-and-error before it can be
stabilized. rDSN provides deterministic execution in simulator so as to repeatedly produce the same
state sequence for easy diagnosis.

```
[core]
tool = simulator

[tools.simulator]
random_seed = 2756568580
```

The non-determinism of the whole system is now determined by a single ```random_seed``` as shown
above. When the seed is non-zero, the system is deterministic as long as the random seed does not
change. When the seed is zero, the system is non-deterministic.

### STEP 7. Scale-up the service

So far, we are using only 1 thread for serving the client requests because
**RPC_COUNTER_COUNTER_ADD** and **RPC_COUNTER_COUNTER_READ** requests are handled by the **default**
thread pool, and in the configuration file, we have **[threadpool.THREAD_POOL_DEFAULT] worker_count
= 1**. To scale up the service, we increase the number of threads to 4. However, the service
handlers in **counter.service.impl.h** must also be made thread safe, by using a lock with type
**::dsn::zrwlock_nr**. In rNET_HDR_DSN, all non-deterministic behaviors must be implemented
using rDSN's service API (see
[here](https://github.com/Microsoft/rDSN/wiki/Programming-Rules-and-FAQ)). However, if you are in
favor of some other reader-writer locks, you can still use them by integrating them as rwlock
providers as we do with logging providers above.

```C++
 10 virtual void on_add(const ::dsn::example::count_op& op, ::dsn::service::rpc_replier<int32_t>&
reply)
 11 {
 12   ::dsn::zauto_write_lock l(_lock);
 13   auto result = (_counters[op.name] += op.operand);
 14   reply(result);
 15 }
 16
 17 virtual void on_read(const std::string& name, ::dsn::service::rpc_replier<int32_t>& reply)
 18 {
 19   ::dsn::zauto_read_lock l(_lock);
 20   auto it = _counters.find(name);
 21   auto result = it != _counters.end() ? it->second : 0;
 22   reply(result);
 23 }
 24
 25 private:
 26 ::dsn::zrwlock_nr _lock;
```

Another approach without using locks is to partition the counter name space so that the **ADD** and
**READ** with the same counter name will always be executed by a same thread. rDSN supports this by
introducing **partitioned** thread pools, which is simply enabled by setting
**[threadpool.SOME_POOL_CODE] partitioned = true**. When emitting a task to be handled by a
partitioned thread pool, a **hash** value needs to be specified so that the thread with index **hash
% worker_count** will serve the task. You can find this parameter in our generate client code.

At this point, we want to introduce **declarative configuration among CPU cores, threads, and
tasks** in rNET_HDR_DSN, which is important when there are a lot more kinds of tasks in the system.
Following is an example (not for our tutorial) where on a machine with four CPU cores, we define two
thread pools, and they use one and three cores respectively to avoid interference (e.g., many
RPC_COUNTER_READ pending tasks block execution of RPC_COUNTER_ADD). Note we also change the default
timeout and channel for RPC calls.

```
[threadpool.THREAD_POOL_COUNTER_DEFAULT]
name = counter update
worker_count = 1
worker_affinity_mask = 0x1 # first core

[threadpool.THREAD_POOL_DEFAULT]
name = counter read
worker_count = 6
worker_affinity_mask = 0xE # later three core

[task.RPC_COUNTER_ADD]
pool_code = THREAD_POOL_COUNTER_DEFAULT
rpc_timeout_milliseconds = 1000
rpc_message_channel = RPC_CHANNEL_UDP

[task.RPC_COUNTER_READ]
pool_code = THREAD_POOL_DEFAULT
rpc_timeout_milliseconds = 500
rpc_message_channel = RPC_CHANNEL_UDP
```

### STEP 8. Systematic test against various failures and scheduling decisions

rDSN provides a fault injector together with the simulator to systematically test your system
against various failures (e.g., message delay, message lost, task scheduling re-ordering, disk
failure, very long task execution), to expose early the possible bugs. Note in this case, we first
add an "assert" in our client code to ensure the second read always get the same value as we get
with the first add operation when both error codes are ok. Then we set client count in configuration
to 2 to see what happens.

```C++
void on_test_timer()
{
    int32_t resp2, resp1;
    ::dsn::error_code err1, err2;

    // test for service 'counter'
    {
        count_op req = {"counter1", 1};
        //sync:
        err1 = _counter_client->add(req, resp1);
        std::cout << "call RPC_COUNTER_COUNTER_ADD end, return " << resp1 << ", err = " <<
err1.to_string() << std::endl;
        //async:
        //_counter_client->begin_add(req);

    }
    {
        std::string req = "counter1";
        //sync:
        err2 = _counter_client->read(req, resp2);
        std::cout << "call RPC_COUNTER_COUNTER_READ end, return " << resp2 << ", err = " <<
err2.to_string() << std::endl;
        //async:
        //_counter_client->begin_read(req);

    }
    if (err1 == 0 && err2 == 0)
    {
        assert (resp2 == resp1);
    }
}
```

```
[apps.client]
name = client
type = counter_client
arguments = localhost 27001
count = 2
run = true

[core]
tool = simulator
toollets = tracer, fault_injector

; logging_factory_name = dsn::tools::screen_logger

[tools.simulator]
random_seed = 0
```

Note despite that we have multiple threads in the server serving the requests, the simulator can
still run the whole system as if it were a single thread to ensure a deterministic execution. We set
**use_given_random_seed = false** in this case, so each run of the system is a different sequence.
We usually start many runs simultaneously to speed-up the test. An example can be find
[here](https://github.com/Microsoft/rDSN/blob/master/src/apps/replication/exe/test.cmd) which we use
for testing our replication framework.

If some of the instances expose bugs (by hitting assertions or crashes), the first thing to do is
look into the first logging file log.1.txt, find the random seed for this run, and then go to STEP 6
with bugs reproduced for debugging.

```
00:00:00.000(0) system.io-thrd.5d30: env.provider.simulator, simulation.random seed for this round
is 225948868
```

### STEP 9. Understanding the performance using the profiler tool

rDSN provides the profiler toollet to understand the performance of the system: including latency
measurement for each kind of RPC calls, and throughput measurements for the thread pools as well as
the low level components (e.g., network, disk AIO). Similar to the tracer toollet, here is the way
we enable the profiler. We illustrate how to extract the profiling results next using cli tool
provided by rDSN.

```
[core]

;tool = simulator
tool = nativerun
toollets = profiler
;toollets = tracer, profiler, fault_injector
pause_on_start = false
```

### STEP 10. Operate your system with local and remote cli

The cli tool provides a console with which developers can get information from all tools (cli
exposes API as part of the Tool API interface, so tools can register their commands), and possibly
control their behavior (e.g., switch on/off). Both local and remote cli tools are enabled via
configuration. Note we also need to ensure logs are dumped to files instead of on-screen to avoid
interference with local cli console I/O.

```
[core]
cli_local = true
cli_remote = true
;logging_factory_name = dsn::tools::screen_logger
```

// TODO: cli with fault injection, tracer, and profiler
By using local or remote cli (via our bin tool ```dsn.cli```), you can get a console to query and/or
control the local/remote processes. Note when using the remote cli, you need to use
```[core]tool=nativerun``` so remote cli can connect to it. On both cases, simply type 'help' to
start.

### STEP 11. Check global correctness using global assertion across nodes

// TODO:

### STEP 12. Handle system overloading using admission controller

A simple approach is to limit the upper bound of the task queue size.

```
[threadpool.THREAD_POOL_DEFAULT]
name = default
partitioned = false
```

// TODO: You can also register your own admission controller and set it in the configuration file.

### STEP 13. Plug-in your own low level components (optional)
You probably already notice that there is a configuration above like "[core] logging_factory_name =
dsn::tools::screen_logger". In this case, we are specifying that we would like to use the
**screen_logger** as our logging provider. You may also change it to "dsn::tools::simple_logger" as
a file logger. Or even better, you may already have your own logging system already, and rDSN allows
easy integration as follows.

#### 13.1. Follow the example in **$(rDSN_DIR)/src/tools/common/simple_logger.h/.cpp** to implement
a new logging provider by wrapping your existing logger.

#### 13.2. In counter.main.cpp, include the header file of what you just implemented, and register
the provider to rDSN.

```C++
 11 int main(int argc, char** argv)
 12 {
 13     // register all possible service apps
 14     dsn::service::system::register_service<
::dsn::example::counter_server_app>("counter_server");
 15     dsn::service::system::register_service<
::dsn::example::counter_client_app>("counter_client");
 16
 17     // register all possible tools and toollets
 18     dsn::tools::register_tool<dsn::tools::nativerun>("nativerun");
 19     dsn::tools::register_tool<dsn::tools::simulator>("simulator");
 20     dsn::tools::register_toollet<dsn::tools::tracer>("tracer");
 21     dsn::tools::register_toollet<dsn::tools::profiler>("profiler");
 22     dsn::tools::register_toollet<dsn::tools::fault_injector>("fault_injector");
 23
 24     // register customized components
 25     dsn::tools::register_component_provider<new_logger>("new_logger");
 26
 27     // specify what services and tools will run in config file, then run
 28     dsn::service::system::run("config.ini");
 29     ::getchar();
 30
 31     return 0;
 32 }
```

#### 13.3. In config.ini, specify the logging provider, and it is done.

```
[core]
logging_factory_name = new_logger
```

rDSN is designed to be open, and many of its components can be replaced like this. Check out
[here](https://github.com/Microsoft/rDSN/wiki/Tool-API:-Component-Providers,-Join-Points,-and-State-Extensions)
to see a lot more and follow the examples under **$(rDSN_DIR)/src/tools/common/** to plug-in your
own.

### STEP 14. Connect the service with other languages (optional)

Although the current rDSN only supports C++ to implement the service code, it allows clients to be
in other languages such as Java, C#, Python, or even others. The way it implements those is to allow
inter-operation with clients generated by the existing tools such as Apache Thrift (with the same
.thrift definition).

For this example, you may define a macro called **DSN_NOT_USE_DEFAULT_SERIALIZATION** for
compilation or go to the generated file **counter.types.h**, uncomment the line defining this macro.
The effect is as the name indicates, and it requires **counter_type.h/.cpp** generated by running
**thrift --gen cpp counter.thrift**. Note in this case we also need to configure the network to
accept Thrift network message headers (i.e., %message_format% = thrift).

```
~/projects/rdsn/tutorial$ thrift --gen cpp counter.thrift
~/projects/rdsn/tutorial$ cp gen-cpp/counter_types.* counter/
~/projects/rdsn/tutorial$ cd counter/build
~/projects/rdsn/tutorial/counter/build$ make
```

```
[network.27001]
; channel = message_format, network_provider_name, buffer_block_size
;RPC_CHANNEL_TCP = NET_HDR_DSN,dsn::tools::asio_network_provider, 65536
RPC_CHANNEL_TCP = NET_HDR_THRIFT,dsn::tools::asio_network_provider, 65536
```

### STEP 15. Open service with multiple ports (optional)

rDSN also supports opening multiple ports for the same services, and each port may allow different
data transmission protocols. Specifically, the message headers can be different, but the data
encoding/decoding must be the same (so far). One possible scenario is that we open a dedicated port
for external request handling (possibly with a different message protocol). Following is an example
where we open two ports for our sample application, and allow both rDSN's message protocol and
standard Thrift binary protocol.

```
[apps.counter.server]
name = counter.server
type = counter_server
arguments =
ports = 27001,27002
run = true

[network.27001]
; channel = message_format, network_provider_name, buffer_block_size
RPC_CHANNEL_TCP = NET_HDR_DSN,dsn::tools::asio_network_provider, 65536

[network.27002]
; channel = message_format, network_provider_name, buffer_block_size
RPC_CHANNEL_TCP = NET_HDR_THRIFT,dsn::tools::asio_network_provider, 65536
```

### STEP 16. Find more information about the config.ini (optional)

Please click [here](https://github.com/Microsoft/rDSN/wiki/Find-useful-information-of-config.ini)

 @}
 */
