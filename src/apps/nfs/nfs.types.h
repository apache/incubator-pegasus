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
# include "nfs_types.h" 


# else // use rDSN's data encoding/decoding

namespace dsn { namespace service { 
    // ---------- copy_request -------------
    struct copy_request
    {
        ::dsn::rpc_address source;
        std::string source_dir;
        std::string dst_dir;
        std::string file_name;
        int64_t offset;
        int32_t size;
        bool is_last;
        bool overwrite;
    };

    inline void marshall(::dsn::binary_writer& writer, const copy_request& val)
    {
        marshall(writer, val.source);
        marshall(writer, val.source_dir);
        marshall(writer, val.dst_dir);
        marshall(writer, val.file_name);
        marshall(writer, val.offset);
        marshall(writer, val.size);
        marshall(writer, val.is_last);
        marshall(writer, val.overwrite);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ copy_request& val)
    {
        unmarshall(reader, val.source);
        unmarshall(reader, val.source_dir);
        unmarshall(reader, val.dst_dir);
        unmarshall(reader, val.file_name);
        unmarshall(reader, val.offset);
        unmarshall(reader, val.size);
        unmarshall(reader, val.is_last);
        unmarshall(reader, val.overwrite);
    }

    // ---------- copy_response -------------
    struct copy_response
    {
        ::dsn::error_code error;
        ::dsn::blob file_content;
        int64_t offset;
        int32_t size;
    };

    inline void marshall(::dsn::binary_writer& writer, const copy_response& val)
    {
        marshall(writer, val.error);
        marshall(writer, val.file_content);
        marshall(writer, val.offset);
        marshall(writer, val.size);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ copy_response& val)
    {
        unmarshall(reader, val.error);
        unmarshall(reader, val.file_content);
        unmarshall(reader, val.offset);
        unmarshall(reader, val.size);
    }

    // ---------- get_file_size_request -------------
    struct get_file_size_request
    {
        ::dsn::rpc_address source;
        std::string dst_dir;
        std::vector< std::string> file_list;
        std::string source_dir;
        bool overwrite;
    };

    inline void marshall(::dsn::binary_writer& writer, const get_file_size_request& val)
    {
        marshall(writer, val.source);
        marshall(writer, val.dst_dir);
        marshall(writer, val.file_list);
        marshall(writer, val.source_dir);
        marshall(writer, val.overwrite);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ get_file_size_request& val)
    {
        unmarshall(reader, val.source);
        unmarshall(reader, val.dst_dir);
        unmarshall(reader, val.file_list);
        unmarshall(reader, val.source_dir);
        unmarshall(reader, val.overwrite);
    }

    // ---------- get_file_size_response -------------
    struct get_file_size_response
    {
        int32_t error;
        std::vector< std::string> file_list;
        std::vector< int64_t> size_list;
    };

    inline void marshall(::dsn::binary_writer& writer, const get_file_size_response& val)
    {
        marshall(writer, val.error);
        marshall(writer, val.file_list);
        marshall(writer, val.size_list);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ get_file_size_response& val)
    {
        unmarshall(reader, val.error);
        unmarshall(reader, val.file_list);
        unmarshall(reader, val.size_list);
    }

} } 

#endif 
