The integration requires two steps:
* registers all service and tool roles that may be used in your system to Zion's runtime
* configure what services and tools to run for each process 

### Service/tool registration
Following is an example where we registers all service and tool roles into a same process with Zion, so that at runtime, any combination of the service instances and/or tool instances can be configured for certain purposes, e.g., global assertion across service nodes, serialized replay of the whole distributed system for debugging. Note **echo_client_app** and **echo_server_app** are service roles that will be discussed below.

```C++
int main(int argc, char * argv[])
{
    // register all possible service roles
    zion::service::system::register_service<echo_client_app>();
    zion::service::system::register_service<echo_server_app>();

    // register all possible tool and toollet roles
    zion::tools::register_tool<zion::tools::nativerun>();
    zion::tools::register_tool<zion::tools::simulator>();    
    zion::tools::register_toollet<zion::tools::tracer>();
    zion::tools::register_toollet<zion::tools::profiler>();
    zion::tools::register_toollet<zion::tools::fault_injector>();
        
    // specify what services and tools will run in config files
    zion::service::system::initialize("echo.ini");
    
    // run the system
    zion::service::system::run();
    ::getchar();
    return 0;
}
```
**echo_server_app** and **echo_client_app** represents the two service roles, we take echo_client_app as an example and show its code below. The key here is to implement **start** and **stop** so our tools are able to do certain fancy things there.

```C++

class echo_client_app : public service_app
{
public:
    echo_client_app(service_app_spec* s, configuration_ptr c)
        : service_app(s, c)
    {
        _client = nullptr;
    }

    virtual ~echo_client_app(void)
    {
    }

    virtual error_code start(int argc, char** argv)
    {
        if (argc < 2)
            return ERR_INVALID_PARAMETERS;

        _client = new echo_client(argv[0], (uint16_t)atoi(argv[1]));
        return ERR_SUCCESS;
    }

    virtual void stop(bool cleanup = false)
    {
        delete _client;
        _client = nullptr;
    }

private:
    echo_client *_client;
};
```

### Service/tool configuration

For each service process with Zion, developers specify what services and what tools are running inside it. Following is an example where an echo client service is running with a set of tools.
```
# in configuration file  echo.ini
[core]
tool = zion::tools::nativerun
; tool = zion::tools::simulator
toollets = zion::tools::tracer, zion::tools::profiler, zion::tools::fault_injector

[apps.echo.client]
name = echo client
type = echo_client_app
arguments = localhost 9001
run = true
count = 1
    
[apps.echo.server]
name = echo server
type = echo_server_app
port = 9001
run = false
```
Zion allows multiple services running in the same process to enable certain tools (e.g., global assert), or improve the performance (e.g., simulated network to reduce network cost). In the above example, developers can set **run** to **true** for all **[apps.___]**. They may also launch multiple instances of certain type of services, e.g., [apps.echo_client].count = 2.



