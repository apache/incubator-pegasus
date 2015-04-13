Robust Distributed System Nucleus (rDSN) is an open framework for quickly building robust and high performance distributed systems. Besides **[Programmability](https://github.com/imzhenyu/rDSN/wiki#programming)** and **[High-Performance](https://github.com/imzhenyu/rDSN/wiki#high-performance)** that many other frameworks focus on, rDSN provides a holistic solution to also cover other important topics that occur throughout the whole lifetime of a distributed system's development and operation, such as **[Testability](https://github.com/imzhenyu/rDSN/wiki#test)**, **[Debuggability](https://github.com/imzhenyu/rDSN/wiki#debug)**, **[Flexiable-Deployment](https://github.com/imzhenyu/rDSN/wiki#deployment)**, **[Scalability](https://github.com/imzhenyu/rDSN/wiki#scale-out)**, and **[High-Availability](https://github.com/imzhenyu/rDSN/wiki#high-availability)**. The system has been used and validated in production inside Microsoft.

## Quick Links

* [Introduction](https://github.com/imzhenyu/rDSN/wiki/Introduction)
* [Installation](https://github.com/imzhenyu/rDSN/wiki/Installation)
* [Tutorial](https://github.com/imzhenyu/rDSN/wiki/Tutorial)
* [Architecture](https://github.com/imzhenyu/rDSN/wiki/Architecture)
* [Contribute to rDSN](https://github.com/imzhenyu/rDSN/wiki/Contribute)
* [FAQ](https://github.com/imzhenyu/rDSN/wiki/FAQ)

## Goals and Features

A tutorial is [here](https://github.com/imzhenyu/rDSN/wiki/Tutorial) to go through most of these features.

#### Programming
* Code generation (for simple/partitioned/replication/test ...)
* Use existing IDL format and data management (Thrift/Protobuf/Bond/...)
* Cross-language inter-operation
* Integrate existing libraries (e.g., your own logging system)
* Dedicated tool API for tool and runtime policy development

#### High performance
* Event-driven architecture (See SEDA)
* Key performance indicator profiling (e.g., qps, exec/queuing/network time)
* Declarative resource planning (how much resource is assigned to certain code)
* Traffic flow control
* Integrate other high-performance components (e.g., lock/network/aio/queue) 

#### Test
* Fault injection
* Global assertion across nodes
* Model checking

#### Debug
* Single process multi-node simulation
* Complete execution flow logging
* Replay for reproducing the bugs
* Progressive debugging

#### Deployment
* Single binary, multiple deployment

#### Scale-out
* Partitioned service management
* Unchanged service interface

#### High-availability
* Partitioned and replicated service management
* Automated replication for each partition
* Unchanged service interface

## License and Support

rDSN is provided in the MIT open source license.

You can use the "issues" tab in github to report bugs. For non-bug issues, please send email to rdsn-support@googlegroups.com.





