/*!
 @defgroup example-network Example
 @ingroup ext

 Plugin A New Network Implementation

 rDSN is designed to be extensible, see
[here](https://github.com/Microsoft/rDSN/wiki/Tool-API:-Component-Providers,-Join-Points,-and-State-Extensions)
for details. This tutorial illustrates how we can plugin a new network implementation for rDSN, due
to higher performance or personal favor. Note the new plugin will also be able to contribute to
other rDSN developers once [contributing] (https://github.com/Microsoft/rDSN/wiki/Contribute) to
rDSN.

### Preliminaries

rDSN supports many networks running concurrently at the same time by allowing multiple ports
specified and configured differently. For each network, it configures the following dimensions of
properties:

* network channel

```C++
DEFINE_CUSTOMIZED_ID_TYPE(rpc_channel)
DEFINE_CUSTOMIZED_ID(rpc_channel, RPC_CHANNEL_TCP)
DEFINE_CUSTOMIZED_ID(rpc_channel, RPC_CHANNEL_UDP)
```

A network can usually support one type of channel, e.g. tcp, udp, http, websocket, or rdma. For
certain networks, e.g., the simulation network, it can easily support all of them.

* network message header format

```C++
DEFINE_CUSTOMIZED_ID_TYPE(network_header_format);
DEFINE_CUSTOMIZED_ID(network_header_format, NET_HDR_DSN);
DEFINE_CUSTOMIZED_ID(network_header_format, NET_HDR_THRIFT);
```

A network can usually support all types of header format, by adopting different
[message_parser](https://github.com/Microsoft/rDSN/blob/master/include/dsn/internal/message_parser.h#L64)s.
A message parser prepares binary blobs from
[rpc_message](https://github.com/Microsoft/rDSN/blob/master/include/dsn/internal/rpc_message.h#L85)
for sending, and composes rpc_message from incoming binary blobs from network channels.

* network message body encoding

The encoding of network message body is handled by upper layer (syntactic sugar library layer, i.e.,
dsn.core now), so it is not a concern of network implementation so far.

A network instance takes charge of two things given a configured network channel and a listening
port:

* send the message to a designated address

* for client calls with a not-null **callback** specified, a network should invoke the callback
either on timeout or when the response message from remote peer is received. To assist this feature,
rDSN provides a class called
[rpc_client_matcher](https://github.com/Microsoft/rDSN/blob/master/include/dsn/internal/network.h#L117),
where it provides two methods to be called on message send and receive, and it handles timeout
internally so developers don't bother.

### STEP 1. define your network

Developers choose what kind of network channel to support by this network (e.g., RPC_CHANNEL_TCP),
and when necessary, define new network channel by

```C++
DEFINE_CUSTOMIZED_ID(rpc_channel, RPC_CHANNEL_HTTP)
```

The base interface for network is
[network](https://github.com/Microsoft/rDSN/blob/master/include/dsn/internal/network.h#L41). When it
is started, it is give the configured network channel, as well as the listen port. For ease of
development for connection oriented network, rDSN further provides
[connection_oriented_network](https://github.com/Microsoft/rDSN/blob/master/include/dsn/internal/network.h#L153),
where it handles management of
[rpc_client_session](https://github.com/Microsoft/rDSN/blob/master/include/dsn/internal/network.h#L187)
and
[rpc_server_session](https://github.com/Microsoft/rDSN/blob/master/include/dsn/internal/network.h#L210).

By selecting and implementing the appropriate base interface, developers implement their own
network. Examples are
[here](https://github.com/Microsoft/rDSN/blob/master/src/tools/common/network.sim.h) and
[here](https://github.com/Microsoft/rDSN/blob/master/src/tools/common/net_provider.h).

### STEP 2. register your network

After implementation, developers register the network into rDSN.

```C++
register_component_provider<asio_network_provider>("dsn::tools::asio_network_provider");
register_component_provider<sim_network_provider>("dsn::tools::sim_network_provider");
```

### STEP 3. use your network

Specify the network provider in your configuration file as follows; note rDSN supports multiple
networks running concurrently with different configurations. For each service app:

```
[apps.default]
network.client.RPC_CHANNEL_TCP = dsn::tools::sim_network_provider, 65536
network.client.RPC_CHANNEL_UDP = dsn::tools::sim_network_provider, 65536
network.server.0.RPC_CHANNEL_TCP = NET_HDR_DSN, dsn::tools::sim_network_provider, 65536
network.server.0.RPC_CHANNEL_UDP = NET_HDR_DSN, dsn::tools::sim_network_provider, 65536

[apps.replica]
ports = 30601,30602
network.server.30601.RPC_CHANNEL_TCP = NET_HDR_DSN, dsn::tools::sim_network_provider, 65536
network.server.30602.RPC_CHANNEL_TCP = NET_HDR_THRIFT, dsn::tools::asio_network_provider, 65536
```
rDSN will create a whole set of network servers and clients using the above settings accordingly.
For each type of RPC (client) calls, you can further specify the channel and message header format
for them.

```
[task.default]
rpc_call_channel = RPC_CHANNEL_TCP
rpc_call_header_format = NET_HDR_DSN

[task.RPC_FD_BEACON]
rpc_call_channel = RPC_CHANNEL_UDP
```



 */
