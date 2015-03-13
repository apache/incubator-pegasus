What is the major challenge for a distributed system? Considering a single-thread sequential program, the main obstacle for developing and operating a distributed system is the non-deterministic behavior such as concurrency, asynchrony, failure, resource interference, and other environmental variance, leading to numerous correctness and performance issues that cost most of the development and operation time.

**Robust Distributed System Nucleus (rDSN)** is a framework for quickly building robust and high performance distributed systems. It has been used and validated in production with thousands of machines. It achieves robustness and high performance through disciplined API design to capture & expose all application level non-deterministic behavior to the rDSN runtime, so that the latter gets full awareness and control of them, which is further manifested as an integrated set of local libraries, distributed frameworks, and tools as well as runtime policies for high-quality system development and/or operation:

* **rDSN.local** is a single machine runtime library, resembling the features such as those in [SEDA](http://www.eecs.harvard.edu/~mdw/proj/seda/) and [Apache Thrift](https://thrift.apache.org/). It exposes a Service API around an unified task execution model, aiming at **programming agility and high performance**. 

* **rDSN.tools** provides a set of tools/runtime policies improving how to better test, debug, deploy, online control, monitor, and even reasoning the distributed systems. The goal is **robustness** and rDSN introduces a Tool API for writing more tools and runtime policies.

* **rDSN.replication** is our first production example using rDSN.local, which is a general-purpose replication framework that makes services scalable, reliable, and highly-available.

Our design ensures that the above aspects are developed independently while they can be seamlessly integrated, i.e., almost all tools and distributed frameworks can be applied to any distributed systems atop of rDSN. The current wiki focuses on the first two aspects.

## Features
***
#### Features for writing the code quickly
* IDL for describing service interface
* Code generation and initial executable ready within seconds
* Rich API support such as RPC, thread pool, synchronization, aio, perf-counter, logging, configuration
* Compatible client access using Thrift in many languages

#### Features for high performance
* Easy plug-in with customized (high-performance) library and runtime policy (e.g., lock/network/thread/queue/throttling)
* Global resource planning and flow control

#### Features for test and debug
* Single process multi-node simulation/deployment
* Automated logging
* Automated profiling
* Automated fault-injection
* Global assertion across nodes
* Replay for reproducing the bugs
* Progressive debugging

#### Other advanced features
* API for writing new tools
* Contribute to the community easily
* ...

### License and Support

