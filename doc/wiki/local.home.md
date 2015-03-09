### Quick links

* [scenario example](https://github.com/imzhenyu/zion/wiki#scenario-example)
* getting started
* [service development walk-through](https://github.com/imzhenyu/zion/wiki/Writing-a-service)
* tool development walk-through
* [service/tool integration walk-through](https://github.com/imzhenyu/zion/wiki/Service-tool-integration)
* available tools

### What is Zion?

Zion is a runtime library (in C++) for **quickly building robust and high performance network service**, resembling the features such as those in **[SEDA](http://www.eecs.harvard.edu/~mdw/proj/seda/)** and **[Apache Thrift](https://thrift.apache.org/)**. It has been **used and validated in production** that runs on thousands of machines.

<I color=red><B>So what's unique here?</B></I> 

Besides its own programming agility and high performance, development of a network service also involves many other jobs like **how to better test, debug, deploy, online control, monitor, and reasoning**. Despite that there are lots of independent work that may help, the integration has never been smooth due to the after-thought of them (e.g., tools not easily applied to services and you have to do the jobs in ad-hoc ways by your own). Zion focuses on providing an open contract among service and tool/runtime policy development (w/ co-designed [Service API](https://github.com/imzhenyu/zion/wiki/Writing-a-service#service-api) and [Tool API](https://github.com/imzhenyu/zion/wiki/Writing-a-tool#tool-api)), so that the development remains independent, while the resulting services and tools can be seamlessly integrated - saving a great deal in the whole life time of a network service.

### Benefit for service developers

Besides benefit like programming agility, Zion provides further a set of unique values to you.

* use of many existing tools by simply adding their names in the configuration file (e.g., automated tracing/profiling/fault injection/systematic testing/replay/all-in-one-process simulation/...)
* plug-in your own primitives when you are not satisfied with our default ones, such as a smarter lock, a better network library, a file IO provider, or even a much lightweight task queue. 
* contribute to others easily. If you plug-in your own primitives, you have a way to contribute all other service developers by simply allowing that.

A technical report of an earlier version of Zion can be found [here](http://research.microsoft.com/apps/pubs/default.aspx?id=232500).

### Scenario example

Following is an example when at a single-box development stage for a replicated service with 1 meta server, 3 replica server, and 2 clients, the developer enables all-in-one-process simulator (w/ replay), tracer, profiling, and fault injection, to expose correctness and performance issues quickly.

```
[core]
tool = zion::tools::simulator
;tool = zion::tools::nativerun
toollets = zion::tools::tracer, zion::tools::profiler, zion::tools::fault_injector

[apps.metaserver]
name = metaserver
type = meta_service_app
port = 20601
    
[apps.replicaserver]
name = replicaserver
type = zion::replication::replication_service_app
port = 20801
count = 3

[apps.client]
name = client
type = app_client_example1_service
arguments =
port = 20911
run = true
count = 2 
```