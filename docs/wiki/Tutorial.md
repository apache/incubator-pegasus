We will develop & operate a counter service here and go through most of rDSN's features. 

### Write the interface, and build, and run!

##### Step 1

Use [Thrift IDL](https://thrift.apache.org/docs/idl) to describe the service interface, saved in counter.dsn.
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

##### Step 2

Use the [enhanced **thrift** tool](xxx) to generate all code, makefile and configuration files.

```bash
~/projects/rdsn$ ./thrift --gen rdsn counter.dsn
~/projects/rdsn/gen-rdsn$ ls
config.ini             counter_constants.h  counter.rdsn.h         counter_types.cpp  makefile
counter_constants.cpp  counter_main.cpp     counter_shared_apps.h  counter_types.h    Makefile.nmake
```
An introduction about these files. 

* counter_main.cpp - the main program, which defines the possible service and tool roles in the final executable. In this case, the counter service and all the default tools are registered. 
* config.ini - configuration file specifying what are the role instances for services and tools for a particular running process.
* counter.rdsn.h - defines both the server and client base class for counter service. 
* counter_shared_apps.h - developers may choose multiple services running as a single service app and multiple service clients in the same node too. This gives you examples (and used in our sample main).
* counter_types.* - how to marshall and unmarshall the request/response messages across network.
* counter_constants.* - defines the constant values appeared in IDL file (not at all in this case).
* makefile, Makefile.nmake - makefile for Linux/Mac and Windows, respectively. 

##### Step 3

Build and run

```bash
~/projects/rdsn$ mv gen-rdsn/ src/apps/counter // due to our current generated makefile
~/projects/rdsn/src/apps/counter$ make -j
~/projects/rdsn/bin$ cp ../src/apps/counter/config.ini .
~/projects/rdsn/bin$ ./counter
----- Random seed for this round is -1538398716
unknown rpc node started @ zhenyuvm1:1 (client only) ...
unknown rpc node started @ zhenyuvm1:8001 ...
exec rpc handler for RPC_COUNTER_ADD
exec rpc handler for RPC_COUNTER_READ
reply ok
reply ok
...
```

### Implement business logic

In order to plug-in the real business logic, we need to write a new service class inheriting **counter_service** generated in counter.rdsn.h. 

```C++
class counter_service_impl : public counter_service
{
public:
    virtual void add(const count_op& op, ::dsn::service::rpc_replier<int32_t>& reply)
    {
        ::dsn::service::zauto_write_lock l(_lock);
        _counters[op.name] += op.operand;
    }
    ...
private:
    ::dsn::service::zrwlock _lock;
    std::map<std::string, long> _counters;
};
```

After that, we need to change the type of **_counter_svc** in **counter_server_app** accordingly so things will work. Note that we use **zrwlock** in this case, which is provided by our Service API. 

### Run differently via changing the configuration file

The generated 'main' function in rDSN actually only registers all the service (server/client) and tool **role**s to rDSN.

```C++
 12 int main(int argc, char** argv)
 13 {
 14     // register all possible service apps
 15     // may replace counter_service using T : public counter_service
 16     dsn::service::system::register_service<counter_service>("counter_server");
 17     dsn::service::system::register_service<counter_client_app>("counter_client");
 18
 19     // register all possible tools and toollets
 20     dsn::tools::register_tool<dsn::tools::nativerun>("nativerun");
 21     dsn::tools::register_tool<dsn::tools::simulator>("simulator");
 22     dsn::tools::register_toollet<dsn::tools::tracer>("tracer");
 23     dsn::tools::register_toollet<dsn::tools::profiler>("profiler");
 24     dsn::tools::register_toollet<dsn::tools::fault_injector>("fault_injector");
 25
 26     // specify what services and tools will run in config file, then run
 27     dsn::service::system::run("config.ini");
 28     ::getchar();
 29
 30     return 0;
 31 }
```

The configuration file specifies what are the concrete instances to run for a particular running process. 

```
[apps.counter.server]
name = counter.server
type = counter_server
port = 8001
run = true

[apps.counter.client]
name = counter.client
type = counter_client
arguments = localhost 8001
run = true
count = 2

[core]
;tool = simulator
tool = nativerun
toollets = tracer, profiler, fault_injector
pause_on_start = false
```
In this case, one server and two clients (all called **service apps**) are running in the same process. We did not specify **port** for clients because these two service apps does not require listen on a particular port. 

We can change the configuration for running server or client only, and use two separated processes so that they may be deployed onto two different machines for real deployment. In this case, we need to change **[core].tool** from **simulator** to **nativerun** so the network becomes real to transmit remote messages.

You may also notice that there are **tool** and **toollet** in the [core] section, which specify all the tools to be loaded at runtime. Tools are conflicting to each other and they cannot co-exist in the same process, and toollets are add-ons and they can be loaded together (in most cases, exceptions are described for each toollet). 

### Resource planning

When large system gets wrong (either performance or correctness issues), it is usually not easy to figure out the root cause when we lack a clear understanding of how the system works as a whole, especially those complicated flows and implicit dependencies. Resource planning helps you get full awareness and control of what are the computations in your system, what are their dependencies, and how the resources are distributed.

As rDSN adopts the **event-driven programming** model, computation in a service are decomposed into many events, or tasks, each of which is a contiguous, sequential execution in one single thread. Examples in our example includes the timer task **LPC_COUNTER_CLIENT_TEST_TIMER**, the RPC request handler task **RPC_COUNTER_ADD**, **RPC_COUNTER_READ**, and their correspondent RPC response or timeout handler tasks. Concurrency is achieved by running many threads simultaneously; each takes tasks from private or shared task queues as workloads.

rDSN allows **declarative configuration among CPU cores, threads, and tasks**. Following is an example where on a machine with four CPU cores, we define two thread pools, and they use one and three cores respectively to avoid interference (e.g., many RPC_COUNTER_READ pending tasks block execution of RPC_COUNTER_ADD). Note we also change the default timeout and channel for RPC calls.

```
// in config.ini
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

### Understand what is happening with logging

Logging is still the most widely adopted mechanism to understand what happens in the system. Instead of manually and intensively writing **printf** in code, the **tracer** tool provides automatic logging with fine-grain configuration so many logs can be turned on/off, shown below. We turn on tracing for all tasks by default, and put LPC_COUNTER_TEST_CLIENT_TIMER as an exception because we are confident it will run as expected.

```
[core]
toollets = tracer

[task.default]
is_trace = true

[task.LPC_COUNTER_TEST_CLIENT_TIMER]
is_trace = false
```
Re-running the counter program outputs logs to screen like this. Our tracing tool instruments all the asynchronous points in the system, therefore in this case you can easily see how the RPC_COUNTER_ADD request is processed in the system.
```
1:   RPC_COUNTER_ADD     RPC.CALL: zhenyuvm1:1 => localhost:8001, rpc_id = xxx ...
8001:RPC_COUNTER_ADD     RPC.REQUEST.ENQUEUE, rpc_id = xxx ...
8001:RPC_COUNTER_ADD     EXEC BEGIN, rpc_id = xxx ...
8001:RPC_COUNTER_ADD     EXEC RPC.REPLY, zhenyuvm1:8001 => 127.0.0.1:1, rpc_id = xxx ...
8001:RPC_COUNTER_ADD     EXEC END, rpc_id = xxx ...
1:   RPC_COUNTER_ADD_ACK RPC.RESPONSE.ENQUEUE, localhost:8001 => zhenyuvm1:1, rpc_id = xxx ...
1:   RPC_COUNTER_ADD_ACK EXEC BEGIN, rpc_id = xxx ...
1:   RPC_COUNTER_ADD_ACK EXEC END, rpc_id = xxx ...
```
### Plug-in your own logging system
Our default logging system outputs everything to the screen, including what we have just got above using the tracer tool. In many cases, developers have their own logging system already, and rDSN allows easy integration as follows. 

1. Follow the example in $(rDSN_DIR)/src/tools/common/logger.screen.h to implement a new logging provider.
2. In counter_main.cpp, include the header file of what you just implemented, and register the provider to rDSN.
```C++
::dsn::tools::register_component_provider<new_logger>("new_logger");
```
3. In config.ini, specify the logging provider, and it is done. 
```bash
[core]
logging_factory_name = new_logger
```
#
<!-- comments below
 and initial executable ready within seconds
* Compatible client access using Thrift in many languages
* Rich API support such as RPC, thread pool, synchronization, aio, perf-counter, logging, configuration
* Plug-in your own API implementation (e.g., your own logging system)

#### Features for high performance
* Automated profiling
* Global resource planning and flow control
* Plug-in your own high-performance library and runtime policy (e.g., lock/network/aio/queue/throttling)

#### Features for test and debug
* Single process multi-node simulation/deployment
* Automated logging
* Automated fault-injection
* Global assertion across nodes
* Replay for reproducing the bugs
* Diff-based debugging

#### Other advanced features
* API for writing new tools
* Contribute to the community easily
* ...
-->
