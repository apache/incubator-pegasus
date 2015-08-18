# pragma once

//
// uncomment the following line if you want to use 
// data encoding/decoding from the original tool instead of rDSN
// in this case, you need to use these tools to generate
// type files with --gen=cpp etc. options
//
// !!! WARNING: not feasible for replicated service yet!!! 
//
// # define DSN_NOT_USE_DEFAULT_SERIALIZATION

# include <dsn/service_api_cpp.h>

# ifdef DSN_NOT_USE_DEFAULT_SERIALIZATION

# include <dsn/thrift_helper.h>
# include "fd_types.h" 

namespace dsn { namespace fd { 
    // ---------- beacon_msg -------------
    inline void marshall(::dsn::binary_writer& writer, const beacon_msg& val)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<beacon_msg>(&proto, val, &beacon_msg::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param beacon_msg& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<beacon_msg>(&proto, val, &beacon_msg::read);
    };

    // ---------- beacon_ack -------------
    inline void marshall(::dsn::binary_writer& writer, const beacon_ack& val)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<beacon_ack>(&proto, val, &beacon_ack::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param beacon_ack& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<beacon_ack>(&proto, val, &beacon_ack::read);
    };

} } 


# else // use rDSN's data encoding/decoding

namespace dsn { namespace fd { 
    // ---------- beacon_msg -------------
    struct beacon_msg
    {
        int64_t time;
        ::dsn::end_point from;
        ::dsn::end_point to;
    };

    inline void marshall(::dsn::binary_writer& writer, const beacon_msg& val)
    {
        marshall(writer, val.time);
        marshall(writer, val.from);
        marshall(writer, val.to);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param beacon_msg& val)
    {
        unmarshall(reader, val.time);
        unmarshall(reader, val.from);
        unmarshall(reader, val.to);
    };

    // ---------- beacon_ack -------------
    struct beacon_ack
    {
        int64_t time;
        ::dsn::end_point this_node;
        ::dsn::end_point primary_node;
        bool is_master;
        bool allowed;
    };

    inline void marshall(::dsn::binary_writer& writer, const beacon_ack& val)
    {
        marshall(writer, val.time);
        marshall(writer, val.this_node);
        marshall(writer, val.primary_node);
        marshall(writer, val.is_master);
        marshall(writer, val.allowed);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param beacon_ack& val)
    {
        unmarshall(reader, val.time);
        unmarshall(reader, val.this_node);
        unmarshall(reader, val.primary_node);
        unmarshall(reader, val.is_master);
        unmarshall(reader, val.allowed);
    };

} } 

#endif 
