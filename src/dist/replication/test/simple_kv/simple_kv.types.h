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
# include "simple_kv_types.h" 


# else // use rDSN's data encoding/decoding

namespace dsn { namespace replication { namespace test { 
    // ---------- kv_pair -------------
    struct kv_pair
    {
        std::string key;
        std::string value;
    };

    inline void marshall(::dsn::binary_writer& writer, const kv_pair& val)
    {
        marshall(writer, val.key);
        marshall(writer, val.value);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ kv_pair& val)
    {
        unmarshall(reader, val.key);
        unmarshall(reader, val.value);
    }

} } } 

#endif 
