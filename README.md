
## Quick Links

* [Installation](https://github.com/Microsoft/rDSN/wiki/Installation)
* [Tutorial (Developers)](https://github.com/Microsoft/rDSN/wiki/Tutorial-D)
* [Tutorial (Researchers and Tool Developers)](https://github.com/Microsoft/rDSN/wiki/Tutorial-R)
* [Tutorial (Students)](https://github.com/Microsoft/rDSN/wiki/Tutorial-S)
* [Principles and FAQ](https://github.com/Microsoft/rDSN/wiki/Principles)
* [Architecture](https://github.com/Microsoft/rDSN/wiki/Architecture)
* [Contribute to rDSN](https://github.com/Microsoft/rDSN/wiki/Contribute)

***

Robust Distributed System Nucleus (rDSN) is an open framework for quickly building and managing high performance and robust distributed systems. The core is a coherent and principled design that benefits the developers, students, and researchers who are working on distributed systems. Following is an incomplete list.

##### Developers: a framework for quickly building and managing high performance and robust systems.

* basic constructs provided for distributed system development (RPC, task library, synchronization, AIO, etc.)
* multiple-platform support (Linux, Windows, Mac)
* quick prototyping via code generation with Apache Thrift and Google Protocol Buffer, extensible for others
* enhanced RPC library with multi-port, multi-channel, multi-language-client support
* flexible to plugin your own low level constructs (network, logging, task queue, performance counter, etc.)
* traditional development style with minor adjustment for holding certain principles to enable more features
* progressive development via simple configuration to minimize reasoning space when a bug surfaces, e.g., single-thread to multiple-thread, constant message delay to variant ones, even with message lost and other failures
* systematic test against various failures and scheduling decisions, exposing possible bugs early
* reproduce bugs, with all nodes' state in a same process and debug without worrying about false timeouts
* automatic task-level flow tracing and performance profiling
* **automated scale-out (sharding) and reliability (replication) with minor development cost**
* flexible deployment

##### Researchers and Tool-Oriented Developers: a tool platform which easies tool development and enables transparent integration with upper applications.

* reliably expose the dependencies and non-determinisms in the upper systems to the underlying tools
* dedicated Tool API for tool and runtime policy development
* transparent integration of the tools with the upper applications

##### Students: a distributed system learning platform where you can easily simplify, understand and manipulate a system.

* progressive protocol development to align with the courses, e.g., synchronous and reliable messages first, then asynchronous and unreliable 
* easy test, debug, and monitoring for system understanding, as with the developers
* easy further tool development, as with the researchers
