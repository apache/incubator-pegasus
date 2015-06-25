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

# include <dsn/internal/serialization.h>

# ifdef DSN_NOT_USE_DEFAULT_SERIALIZATION

# include <dsn/thrift_helper.h>
# include "cli_types.h" 

namespace dsn { 
    // ---------- command -------------
    inline void marshall(::dsn::binary_writer& writer, const command& val)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<command>(&proto, val, &command::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param command& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<command>(&proto, val, &command::read);
    };

} 


# else // use rDSN's data encoding/decoding

namespace dsn { 
    // ---------- command -------------
    struct command
    {
        std::string cmd;
        std::vector< std::string> arguments;
    };

    inline void marshall(::dsn::binary_writer& writer, const command& val)
    {
        marshall(writer, val.cmd);
        marshall(writer, val.arguments);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param command& val)
    {
        unmarshall(reader, val.cmd);
        unmarshall(reader, val.arguments);
    };

} 

#endif 
