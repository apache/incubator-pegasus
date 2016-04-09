# pragma once
# include <dsn/service_api_cpp.h>

//
// uncomment the following line if you want to use 
// data encoding/decoding from the original tool instead of rDSN
// in this case, you need to use these tools to generate
// type files with --gen=cpp etc. options
//
# if defined(DSN_USE_THRIFT_SERIALIZATION)

# include "simple_kv_types.h"

# elif defined(DSN_USE_PROTO_SERIALIZATION)

# include "simple_kv.pb.h"

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
