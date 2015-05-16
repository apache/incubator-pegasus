
## Quick Links

* [Installation](https://github.com/Microsoft/rDSN/wiki/Installation)
* [Tutorial](https://github.com/Microsoft/rDSN/wiki/A-Tutorial-for-Developers)
* [Architecture](https://github.com/Microsoft/rDSN/wiki/Architecture)
* [Contribute to rDSN](https://github.com/Microsoft/rDSN/wiki/Contribute)
* [Programming Tips and FAQ](https://github.com/Microsoft/rDSN/wiki/Programming-Tips-and-FAQ)

***

Robust Distributed System Nucleus (rDSN) is an open framework for quickly building and managing high performance and robust distributed systems. The key is a coherent and principled design that distributed systems, tools, and frameworks can be developed independently and later on integrated (almost) transparently. Following are some highlights for different audience of this framework. 

##### Developers: a framework for quickly building and managing high performance and robust systems.

* basic constructs provided for distributed system development (RPC, tasking, synchronization, etc.)
* multiple-platform support (Linux, Windows, Mac)
* quick prototyping via code generation with Thrift and Protocol Buffer, extensible for others
* enhanced RPC library with multi-port, multi-channel, multi-language-client support
* flexible to plugin your own low level constructs (network, logging, task queue, lock, etc.)
* progressive system complexity via configuration to minimize reasoning space when a bug surfaces
* systematic test against various failures and scheduling decisions, exposing possible bugs early
* reproduce bugs, with all nodes' state in one process and debug w/o worrying about false timeouts
* automated task-level flow tracing and performance profiling
* **automated partitioning and replication with minor development cost**
* flexible deployment

##### Researchers and tool-oriented developers: a tool platform which easies tool development and enables transparent integration with upper applications.

* reliably expose dependencies and non-determinisms in upper systems to the underlying tools
* dedicated Tool API for tool and runtime policy development
* transparent integration of the tools with the upper applications to make real impact

##### Students: a distributed system learning platform where you can easily simplify, understand and manipulate a system.

* progressive protocol development to align with the courses, e.g., synchronous and reliable messages first, then asynchronous and unreliable 
* easy test, debug, and monitoring for system understanding, as with the developers
* easy further tool development, as with the researchers
