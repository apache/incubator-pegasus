I am a [developer](https://github.com/Microsoft/rDSN#developers-a-framework-for-quickly-building-and-managing-high-performance-and-robust-distributed-systems)  |   [researcher](https://github.com/Microsoft/rDSN#researchers-and-tool-oriented-developers-a-tool-platform-which-eases-tool-development-and-enables-transparent-integration-with-upper-applications-see-tutorial-1-and-tutorial-2)  |   [student](https://github.com/Microsoft/rDSN#students-a-distributed-system-learning-platform-where-you-can-easily-simplify-understand-and-manipulate-a-system-see-tutorial) =>

### See [Wiki](https://github.com/Microsoft/rDSN/wiki) for details  ...

***

![rDSN](https://raw.githubusercontent.com/Microsoft/rDSN/master/resources/rdsn.jpg)
**Robust Distributed System Nucleus (rDSN)** is an open framework for quickly building and managing high performance and robust distributed systems. An early version of rDSN has been **used in Bing** for building a distributed data service, and the system has been online and running well. Based on the feedbacks, rDSN is improved and now [made public](http://research.microsoft.com/en-us/projects/rdsn/default.aspx) with the MIT open source license. The idea is to advocate a [coherent and principled meta stack](https://github.com/Microsoft/rDSN/wiki/Design-Rational) that distributed applications, tools, and frameworks are developed independently and integrated (almost) transparently to benefit each other. Following are some highlights for different audience of this framework.

I am a [developer](https://github.com/Microsoft/rDSN/wiki/overview#developers-a-framework-for-quickly-building-and-managing-high-performance-and-robust-distributed-systems)  |   [researcher](https://github.com/Microsoft/rDSN/wiki/overview#researchers-and-tool-oriented-developers-a-tool-platform-which-eases-tool-development-and-enables-transparent-integration-with-upper-applications-see-tutorial-1-and-tutorial-2)  |   [student](https://github.com/Microsoft/rDSN/wiki/overview#students-a-distributed-system-learning-platform-where-you-can-easily-simplify-understand-and-manipulate-a-system-see-tutorial) =>

***

##### Developers: a framework for quickly building and managing high performance and robust distributed systems.


* Highlights ([more](https://github.com/Microsoft/rDSN/wiki/overview#built-in-three-layer-meta-stack-for-quickly-building-distributed-systems-with-support-from-a-growing-set-of-tools-and-frameworks))
 * compatible code generation via Apache Thrift and Google Protocol Buffer
 * automatic test against various failures and scheduling decisions with reproducable bug
 * flexiable to plugin your own module to adapt to existing culture or for higher performance
 * automatically turn single-node service to a scalable and reliable service with built-in replication
* Have concerns? [=>](https://github.com/Microsoft/rDSN/wiki/overview#quick-development-and-flexible-deployment-with-concerns-addressed)

###### Built-in three-layer meta stack for quickly building distributed systems with support from [a growing set of tools and frameworks](https://github.com/Microsoft/rDSN/wiki/Available-Tools-Policies-and-Frameworks)
 * Layer 1: [quick service development](https://github.com/Microsoft/rDSN/Wiki/Overview#quick-development-and-flexible-deployment-with-concerns-addressed), plus with support from many tools and policies: simulation, fault injection, tracing, profiling, replay, throttling, ...  (see [Tutorial](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service))
 
 ![rDSN-layer1](https://raw.githubusercontent.com/Microsoft/rDSN/master/resources/rdsn-layer1.jpg)
 * Layer 2: to a partitioned? and replicated? service with simple configuration and minor further development cost
 (see [Tutorial](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Scalable-and-Reliable-Counter-Service))

 ![rDSN-layer2](https://raw.githubusercontent.com/Microsoft/rDSN/master/resources/rdsn-layer2.jpg)
 * Layer 3: compose workflow across multiple services in a declarative way to handle end-to-end incoming workloads (coming later)
 
 ![rDSN-layer3](https://raw.githubusercontent.com/Microsoft/rDSN/master/resources/rdsn-layer3.jpg)

###### Quick development and flexible deployment (with concerns addressed)
 * multiple-platform support (Linux, Windows, Mac)
 * Compatible code generation with Thrift and Protocol Buffer, extensible for others
 * [enhanced RPC library](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-14-connect-the-service-with-other-languages-optional) with multi-port, multi-channel, multi-language-client support
 * adapt to your own environment, or rDSN does not perform good enough? rDSN is highly extensible that you can always [plugin your own](https://github.com/Microsoft/rDSN/wiki/Tool-API:-Component-Providers,-Join-Points,-and-State-Extensions#component-providers) low level constructs (network, logging, task queue, lock, performanc counter etc.)
 * [flexible deployment]((https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service#step-3-run-with-the-native-runtime-and-deployment)) - single executable, multiple roles, multiple instances
 * more concerns, please do not hesitate to let us [know](mailto:rdsn-support@googlegroups.com)

***

##### Researchers and tool-oriented developers: a tool platform which eases tool development and enables transparent integration with upper applications. (see [Tutorial 1](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Network-Fault-Injection) and [Tutorial 2](https://github.com/Microsoft/rDSN/wiki/Tutorial:-How-to-Implement-A-Coorperative-Scheduler))

* reliably expose dependencies and non-determinisms in upper systems to the underlying tools at a semantic-rich level (i.e., task)
 * completeness made easy for tools
 * reduced state space for tools to handle
 * easier to map tool results to application logic
* dedicated Tool API for tool and runtime policy development
* transparent integration of the tools with the upper applications to make real impact

***

##### Students: a distributed system learning platform where you can easily simplify, understand and manipulate a system. (see [Tutorial](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Perfect-Failure-Detector))

* configurable system complexity to learn step by step
 * single thread to multiple threads
 * synchronous and reliable messages to asynchronous and unreliable ones
* easy test, debug, and monitoring for system understanding, as with the developers
* easy further tool development, as with the researchers


***


##### License and Support

rDSN is provided in C++ on Windows, Linux, and Mac, with the MIT open source license. You can use the "issues" tab in github to report bugs. For non-bug issues, please send email to rdsn-support@googlegroups.com.

