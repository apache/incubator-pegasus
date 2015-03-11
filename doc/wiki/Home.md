What is the major challenge for a distributed system? Considering a single-thread sequential program, the major obstacle for developing and operating a distributed system is the non-determinisic behaviors such as concurrency, asynchrony, failure, resource interference, and other environmental variance, leading to numerous correctness and performance issues. 

**Robust Distributed System Nucleus (rDSN)** is a portable C++ framework for quickly building robust and high performance distributed systems. It has been used and validated in production with thousands of machines. It achieves robustness and high performance through disciplined API design to capture & expose all application level non-determinisic behaviors to the rDSN runtime, so that the latter gets full awareness and control of them, which is further manifested as an integrated set of local libraries, distributed frameworks, and tools as well as runtime policies for distributed system development and/or operation:

* **rDSN.local** is a single machine runtime library, resembling the features such as those in [SEDA](http://www.eecs.harvard.edu/~mdw/proj/seda/) and [Apache Thrift](https://thrift.apache.org/). It exposes a Service API around an unified task execution model, aiming at **programming agility and high performance**. 

* **rDSN.distributed** contains a set of distributed frameworks, such as partitioning, load-balancing, replication, and failure-detection, to help address the challenges raised by distributed systems. The targets are **scalability, reliability, and high availability**, even with high workload and failure rate.

* **rDSN.tools** provides a set of tools/runtime policies improving how to better test, debug, deploy, online control, monitor, and even reasoning the distributed systems. The goal is **robustness** and rDSN introduces a Tool API for writing more tools and runtime policies.

Note our API is designed in a way that the above aspects are developed independently while they can be seemlessly integrated. That is to say, almost all tools and distributed frameworks can be applied to any distributed systems atop of our Service API.


### Features
* in C++, portable on *inux and Windows
* event-driven programming model
* 

easy to use: 
	- deep customization 
	- flexible configuration
	- inter-operation with Thrift/Protobuf


Challengings regarding to distributed systems compared to single box single thread systems:

* concurrency
* asynchrony 
* failure
* scalability 

Essentially 

