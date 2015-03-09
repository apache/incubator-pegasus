Writing a service atop of Zion requires three major steps:
* ([global planning](https://github.com/imzhenyu/zion/wiki/Writing-a-service#global-planning)) define what are the tasks in your service and how you want to use the computation resources for them 
* (implementation) implement the business logic in your service using Zion's [service API](https://github.com/imzhenyu/zion/wiki/Writing-a-service#service-api)
* ([integration](https://github.com/imzhenyu/zion/wiki/Service-tool-integration)) hook up the service implementation with the tools provided by Zion, including the native runtime so that you can run your service end-to-end

### Example
Before introducing the steps in detail, following is one complete example where an echo service is implemented ([integration](https://github.com/imzhenyu/zion/wiki/Service-tool-integration) is not included). 

```C++
// thread and task definition
DEFINE_THREAD_POOL_CODE(THREAD_POOL_TEST)
DEFINE_TASK_CODE(LPC_ECHO_TIMER, TASK_PRIORITY_HIGH, THREAD_POOL_TEST)
DEFINE_TASK_CODE_RPC(RPC_ECHO, TASK_PRIORITY_HIGH, THREAD_POOL_TEST)

// server
class echo_server : public serviceletex<echo_server>
{
public:
    echo_server() : serviceletex<echo_server>("echo_server") 
    {
        register_rpc_handler(RPC_ECHO, "RPC_ECHO", &echo_server::on_echo);
    }

    void on_echo(const std::string& req, __out std::string& resp)
    {
        resp = req;
    }
};

// client
class echo_client : public serviceletex<echo_client>
{
public:
    echo_client(const end_point& server_addr) : serviceletex<echo_client>("echo_client")
    {
        _server = server_addr;
        enqueue_task(LPC_ECHO_TIMER, &echo_client::on_echo_timer, 0, 0, 1000);
    }

    void on_echo_timer()
    {
        boost::shared_ptr<std::string> req(new std::string("hi, zion"));
        rpc_typed(_server, RPC_ECHO, req, &echo_client::on_echo_reply, 0, 3000);
    }

    void on_echo_reply(error_code err, boost::shared_ptr<std::string> req, boost::shared_ptr<std::string> resp)
    {
        if (err != ERR_SUCCESS) std::cout << "echo err: " << err.to_string() << std::endl;
        else std::cout << "echo result: " << *resp << std::endl;
    }
private:
    end_point _server;
};
```

### Global planning

When large system gets wrong (either performance or correctness issues), it is usually not easy to figure out the root cause when we lack a clear understanding of how the system works as a whole, especially those complicated flows and implicit dependencies. Global planning helps you get full awareness and control of what are the computations in your system, what are their dependencies, and how the resources are distributed.

As Zion adopts the **event-driven programming** model, computation in a service are decomposed into many events, or tasks, each of which is a contiguous, sequential execution in one single thread. There are **four different kinds of tasks in Zion: common computation tasks (including timers), disk IO callback tasks, RPC request handler tasks, and RPC response or RPC timeout handler tasks**. Concurrency is achieved by running many threads simultaneously; each takes tasks from private or shared task queues as workloads.

Zion allows **declarative configuration among CPU cores, threads, and tasks**. Following is an example where on a machine with four CPU cores, we define two thread pools, and they use one and three cores respectively to avoid performance interference. Tasks are all labeled with names, and each name (or kind) is assigned to certain thread pools. 

```C++
// in source code my_service.h
DEFINE_THREAD_POOL_CODE(THREAD_POOL_FD)
DEFINE_THREAD_POOL_CODE(THREAD_POOL_MS)
DEFINE_TASK_CODE_RPC(RPC_BEACON, TASK_PRIORITY_COMMON, THREAD_POOL_FD)
DEFINE_TASK_CODE_RPC(RPC_QUERY_CONFIGURATION, TASK_PRIORITY_COMMON, THREAD_POOL_MS)
DEFINE_TASK_CODE_RPC(RPC_UPDATE_CONFIGURATION, TASK_PRIORITY_COMMON, THREAD_POOL_MS)
```

```
# in configuration file zion.ini
[threadpool.THREAD_POOL_FD]
name = failure detector
worker_count = 1
worker_affinity_mask = 0x1 # first core

[threadpool.THREAD_POOL_MS]
name = meta state management
worker_count = 6
worker_affinity_mask = 0xE # later three core
```
### Service API

The tasks in Zion communicates with each other and the environment through service API, which covers the following five areas. 

* tasking
```C++
void enqueue(task_ptr& task, int delay_milliseconds = 0);
bool cancel(task_ptr& task, bool wait_until_finished);
bool wait(task_ptr& task, int timeout_milliseconds = INFINITE);
```
* rpc
```C++
const end_point& get_local_address();
bool register_rpc_handler(task_code code, 
                          const char* name, 
                          rpc_server_handler* handler);
bool unregister_rpc_handler(task_code code);
rpc_response_task_ptr call(const end_point& server, 
                           message_ptr& request, 
                           rpc_response_task_ptr callback = nullptr);
void reply(message_ptr& response);
```
* file
```C++
handle_t open(const char* file_name, int flag, int pmode);
void read(handle_t file_handle, char* buffer, int count, uint64_t offset, aio_task_ptr& callback);
void write(handle_t file_handle, const char* buffer, int count, uint64_t offset, aio_task_ptr& callback); 
error_code close(handle_t file_handle);
```
* env (environment)
```C++
uint64_t now_ns();
uint64_t random64(uint64_t min, uint64_t max);
```
* synchronization
```C++
class zlock; // exclusive lock or mutex
class zrwlock; // reader-writer lock
class zsemaphore; // semaphore or condition variable
```
Developers may write services with this API directly. Zion also introduces two further higher level abstractions so developers can avoid doing some redundant work:

* **servicelet**. As shown above, **task** is everywhere in the service API, which requires developers to explicitly define task objects in their service. **servicelet** allows developers to refer to the methods in their service context, and the runtime converts them into task objects automatically. Furthermore, the tasks are canceled when the service context is destroyed, so you don't need to worry about invalid service context access by some dangling tasks. 
```C++
template <typename T>
class servicelet : ...
{
public:
  void register_rpc_handler(task_code rpc_code, const char* name_, void (T::*handler)(message_ptr&));
  rpc_response_task_ptr rpc_call(
        const end_point& server_addr,
        message_ptr& request,
        void (T::*callback)(error_code, message_ptr&, message_ptr&),
        int reply_hash = 0
        );
  ...
};
```

* **serviceletex**. You may also notice that **message** is all around which represents a network message where you can **marshall** and **unmarshall** data structures from/to it. **serviceletex** allows **typed** methods around service context, and the marshall/unmarshall are done by the runtime automatically. Developers are required to implement a pair of marshall/unmarshall routines for each RPC request/response data type. The [example](https://github.com/imzhenyu/zion/wiki/Writing-a-service#example) above uses this abstraction.

#### Integration

See [here](https://github.com/imzhenyu/zion/wiki/Service-tool-integration). 
