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
# include "nfs_types.h" 

namespace dsn {
    namespace service {
        // ---------- copy_request -------------
        inline void marshall(::dsn::binary_writer& writer, const copy_request& val)
        {
            boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
            ::apache::thrift::protocol::TBinaryProtocol proto(transport);
            ::dsn::marshall_rpc_args<copy_request>(&proto, val, &copy_request::write);
        };

        inline void unmarshall(::dsn::binary_reader& reader, __out_param copy_request& val)
        {
            boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
            ::apache::thrift::protocol::TBinaryProtocol proto(transport);
            ::dsn::unmarshall_rpc_args<copy_request>(&proto, val, &copy_request::read);
        };

        // ---------- copy_response -------------
        inline void marshall(::dsn::binary_writer& writer, const copy_response& val)
        {
            boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
            ::apache::thrift::protocol::TBinaryProtocol proto(transport);
            ::dsn::marshall_rpc_args<copy_response>(&proto, val, &copy_response::write);
        };

        inline void unmarshall(::dsn::binary_reader& reader, __out_param copy_response& val)
        {
            boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
            ::apache::thrift::protocol::TBinaryProtocol proto(transport);
            ::dsn::unmarshall_rpc_args<copy_response>(&proto, val, &copy_response::read);
        };

        // ---------- get_file_size_request -------------
        inline void marshall(::dsn::binary_writer& writer, const get_file_size_request& val)
        {
            boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
            ::apache::thrift::protocol::TBinaryProtocol proto(transport);
            ::dsn::marshall_rpc_args<get_file_size_request>(&proto, val, &get_file_size_request::write);
        };

        inline void unmarshall(::dsn::binary_reader& reader, __out_param get_file_size_request& val)
        {
            boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
            ::apache::thrift::protocol::TBinaryProtocol proto(transport);
            ::dsn::unmarshall_rpc_args<get_file_size_request>(&proto, val, &get_file_size_request::read);
        };

        // ---------- get_file_size_response -------------
        inline void marshall(::dsn::binary_writer& writer, const get_file_size_response& val)
        {
            boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
            ::apache::thrift::protocol::TBinaryProtocol proto(transport);
            ::dsn::marshall_rpc_args<get_file_size_response>(&proto, val, &get_file_size_response::write);
        };

        inline void unmarshall(::dsn::binary_reader& reader, __out_param get_file_size_response& val)
        {
            boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
            ::apache::thrift::protocol::TBinaryProtocol proto(transport);
            ::dsn::unmarshall_rpc_args<get_file_size_response>(&proto, val, &get_file_size_response::read);
        };

    }
}


# else // use rDSN's data encoding/decoding

namespace dsn {
    namespace service {

        // ---------- copy_request -------------
        struct copy_request
        {
            end_point source;
            std::string source_dir;
            std::string dst_dir;
            std::string file_name;
            uint64_t offset;
            uint32_t size;
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
        };

        inline void unmarshall(::dsn::binary_reader& reader, __out_param copy_request& val)
        {
            unmarshall(reader, val.source);
            unmarshall(reader, val.source_dir);
            unmarshall(reader, val.dst_dir);
            unmarshall(reader, val.file_name);
            unmarshall(reader, val.offset);
            unmarshall(reader, val.size);
            unmarshall(reader, val.is_last);
            unmarshall(reader, val.overwrite);
        };

        // ---------- copy_response -------------
        struct copy_response
        {
            error_code error;
            std::string file_name;
            std::string dst_dir;
            blob file_content;
            uint64_t offset;
            uint32_t size;
        };

        inline void marshall(::dsn::binary_writer& writer, const copy_response& val)
        {
            marshall(writer, val.error);
            marshall(writer, val.file_name);
            marshall(writer, val.dst_dir);
            marshall(writer, val.file_content);
            marshall(writer, val.offset);
            marshall(writer, val.size);
        };

        inline void unmarshall(::dsn::binary_reader& reader, __out_param copy_response& val)
        {
            unmarshall(reader, val.error);
            unmarshall(reader, val.file_name);
            unmarshall(reader, val.dst_dir);
            unmarshall(reader, val.file_content);
            unmarshall(reader, val.offset);
            unmarshall(reader, val.size);
        };

        // ---------- get_file_size_request -------------
        struct get_file_size_request
        {
            end_point source;
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
        };

        inline void unmarshall(::dsn::binary_reader& reader, __out_param get_file_size_request& val)
        {
            unmarshall(reader, val.source);
            unmarshall(reader, val.dst_dir);
            unmarshall(reader, val.file_list);
            unmarshall(reader, val.source_dir);
            unmarshall(reader, val.overwrite);
        };

        // ---------- get_file_size_response -------------
        struct get_file_size_response
        {
            int32_t error;
            std::vector< std::string> file_list;
            std::vector< uint64_t> size_list;
        };

        inline void marshall(::dsn::binary_writer& writer, const get_file_size_response& val)
        {
            marshall(writer, val.error);
            marshall(writer, val.file_list);
            marshall(writer, val.size_list);
        };

        inline void unmarshall(::dsn::binary_reader& reader, __out_param get_file_size_response& val)
        {
            unmarshall(reader, val.error);
            unmarshall(reader, val.file_list);
            unmarshall(reader, val.size_list);
        };

    }
}

#endif 
