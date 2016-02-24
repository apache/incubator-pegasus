# pragma once
# include <dsn/service_api_cpp.h>

//
// uncomment the following line if you want to use 
// data encoding/decoding from the original tool instead of rDSN
// in this case, you need to use these tools to generate
// type files with --gen=cpp etc. options
//
// !!! WARNING: not feasible for replicated service yet!!! 
//
// # define DSN_NOT_USE_DEFAULT_SERIALIZATION

# ifdef DSN_NOT_USE_DEFAULT_SERIALIZATION


# include <dsn/thrift_helper.h>
# include "fd_types.h" 


# else // use rDSN's data encoding/decoding

namespace dsn { namespace fd { 
    // ---------- beacon_msg -------------
    struct beacon_msg
    {
        int64_t time;
        ::dsn::rpc_address from_addr;
        ::dsn::rpc_address to_addr;
    };

    inline void marshall(::dsn::binary_writer& writer, const beacon_msg& val)
    {
        marshall(writer, val.time);
        marshall(writer, val.from_addr);
        marshall(writer, val.to_addr);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ beacon_msg& val)
    {
        unmarshall(reader, val.time);
        unmarshall(reader, val.from_addr);
        unmarshall(reader, val.to_addr);
    }

    // ---------- beacon_ack -------------
    struct beacon_ack
    {
        int64_t time;
        ::dsn::rpc_address this_node;
        ::dsn::rpc_address primary_node;
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
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ beacon_ack& val)
    {
        unmarshall(reader, val.time);
        unmarshall(reader, val.this_node);
        unmarshall(reader, val.primary_node);
        unmarshall(reader, val.is_master);
        unmarshall(reader, val.allowed);
    }

} } 

#endif 
