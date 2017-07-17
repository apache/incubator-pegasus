/*!
 @defgroup l2-model Overview
 @ingroup dev-layer2

 EON (rDSN Layer 2) scales out a local component (either legacy or from rDSN layer 1), and makes it
 reliable automatically, to turn it into a real cloud service. We therefore call the application
model in EON service model.

### Service Properties

In EON, we consider the following properties which largely decides the needed runtime support.

#### Stateless or stateful.

A service is stateless when the result for a service request remains the same when a service is just
started bases on zero state, or running for a long time. A service is otherwise stateful, meaning
that the accumulated state in the services needs to be carefully maintained to ensure correctness of
the service requests. For example, the state must not be lost.

#### Partitioned or not.

When the state for a service is too large to be fit into a single machine's capacity (e.g., memory),
developers usually partition the service according to certain rules (typically range or hash
partitioning), to scale out the service.

#### Replicated or not.

When a service is stateful, replication is required to ensure high availability and reliability by
replicating the state onto multiple copies. Meanwhile, multiple copies usually increase the read
throughput of the system, not matter a service (or a service partition) is stateful or stateless.
Replication has significant impact on how the read or write service calls are handled underneath.
For example, a write service call (e.g., Put) may be propagated to all copies to ensure consistency,
and a read service call (e.g., Get) may be issued to multiple copies for lower latency.

#### Service Model and Runtime Support Frameworks

According to the above service properties, EON separates the services into the following kinds, and
provides runtime frameworks to turn layer 1 applications or legacy local libraries into a real
service.

 */