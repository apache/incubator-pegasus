/*!
@defgroup tutorials Tutorials

Tutorials

Before we start, make sure rDSN has been appropriately [installed](\ref install).

- [application development](#dev-app)
- [framework development](#dev-framework)
- [tool and local runtime library development](#dev-tool)

## <a name="dev-app"> Application development </a>

### A single-node rocksdb service

At its simplest form, rDSN can be used as a library such as libevent, Thrift or GRPC, with the
following advantages:

- compatible code generation with Apache Thrift and Google Protocol Buffer
- enhanced network with concurrent support of mutliple ports, header types, and payload encoding
schema
- rich [Service API](\ref service-api) beyond RPC
- built-in [dev&op support](\ref tools)
- [configurable runtime] (\ref config-runtime)
- easy integration with existing cluster environment

View [tutorial](\ref tutorial-simple) for the details.

### A rocksdb cluster

rDSN has a built-in Paxos framework for quickly turning a local stateful component (e.g., storage)
into a replicated and partitioned online service.

- TODO

View [tutorial](\ref tutorial-replication)

### A memcached cluster

rDSN has a built-in framework for quickly turning a local stateless component into a scalable online
service with automatic failure recovery and load balancing.

- TODO

View [tutorial](\ref tutorial-memcached)

## <a name="dev-framework"> Framework development </a>


## <a name="dev-tool"> Tool and local runtime library development </a>



*/
