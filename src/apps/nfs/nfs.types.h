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

namespace dsn {
    // ---------- copy_request -------------
    template<>
    inline uint32_t marshall_base<service::copy_request>(::apache::thrift::protocol::TProtocol* oprot, const service::copy_request& val)
    {
        uint32_t xfer = 0;
        oprot->incrementInputRecursionDepth();
        xfer += oprot->writeStructBegin("rpc_message");
        xfer += oprot->writeFieldBegin("msg", ::apache::thrift::protocol::T_STRUCT, 1);

        xfer += val.write(oprot);

        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        oprot->decrementInputRecursionDepth();
        return xfer;
    }

    template<>
    inline uint32_t unmarshall_base<service::copy_request>(::apache::thrift::protocol::TProtocol* iprot, /*out*/ service::copy_request& val)
    {
        uint32_t xfer = 0;
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;
        xfer += iprot->readStructBegin(fname);
        using ::apache::thrift::protocol::TProtocolException;
        while (true)
        {
            xfer += iprot->readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }
            switch (fid)
            {
            case 1:
                if (ftype == ::apache::thrift::protocol::T_STRUCT) {
                    xfer += val.read(iprot);
                }
                else {
                    xfer += iprot->skip(ftype);
                }
                break;
            default:
                xfer += iprot->skip(ftype);
                break;
            }
            xfer += iprot->readFieldEnd();
        }
        xfer += iprot->readStructEnd();
        iprot->readMessageEnd();
        iprot->getTransport()->readEnd();
        return xfer;
    }

    // ---------- copy_response -------------
    template<>
    inline uint32_t marshall_base<service::copy_response>(::apache::thrift::protocol::TProtocol* oprot, const service::copy_response& val)
    {
        uint32_t xfer = 0;
        oprot->incrementInputRecursionDepth();
        xfer += oprot->writeStructBegin("rpc_message");
        xfer += oprot->writeFieldBegin("msg", ::apache::thrift::protocol::T_STRUCT, 1);

        xfer += val.write(oprot);

        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        oprot->decrementInputRecursionDepth();
        return xfer;
    }

    template<>
    inline uint32_t unmarshall_base<service::copy_response>(::apache::thrift::protocol::TProtocol* iprot, /*out*/ service::copy_response& val)
    {
        uint32_t xfer = 0;
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;
        xfer += iprot->readStructBegin(fname);
        using ::apache::thrift::protocol::TProtocolException;
        while (true)
        {
            xfer += iprot->readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }
            switch (fid)
            {
            case 1:
                if (ftype == ::apache::thrift::protocol::T_STRUCT) {
                    xfer += val.read(iprot);
                }
                else {
                    xfer += iprot->skip(ftype);
                }
                break;
            default:
                xfer += iprot->skip(ftype);
                break;
            }
            xfer += iprot->readFieldEnd();
        }
        xfer += iprot->readStructEnd();
        iprot->readMessageEnd();
        iprot->getTransport()->readEnd();
        return xfer;
    }

    // ---------- get_file_size_request -------------
    template<>
    inline uint32_t marshall_base<service::get_file_size_request>(::apache::thrift::protocol::TProtocol* oprot, const service::get_file_size_request& val)
    {
        uint32_t xfer = 0;
        oprot->incrementInputRecursionDepth();
        xfer += oprot->writeStructBegin("rpc_message");
        xfer += oprot->writeFieldBegin("msg", ::apache::thrift::protocol::T_STRUCT, 1);

        xfer += val.write(oprot);

        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        oprot->decrementInputRecursionDepth();
        return xfer;
    }

    template<>
    inline uint32_t unmarshall_base<service::get_file_size_request>(::apache::thrift::protocol::TProtocol* iprot, /*out*/ service::get_file_size_request& val)
    {
        uint32_t xfer = 0;
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;
        xfer += iprot->readStructBegin(fname);
        using ::apache::thrift::protocol::TProtocolException;
        while (true)
        {
            xfer += iprot->readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }
            switch (fid)
            {
            case 1:
                if (ftype == ::apache::thrift::protocol::T_STRUCT) {
                    xfer += val.read(iprot);
                }
                else {
                    xfer += iprot->skip(ftype);
                }
                break;
            default:
                xfer += iprot->skip(ftype);
                break;
            }
            xfer += iprot->readFieldEnd();
        }
        xfer += iprot->readStructEnd();
        iprot->readMessageEnd();
        iprot->getTransport()->readEnd();
        return xfer;
    }

    // ---------- get_file_size_response -------------
    template<>
    inline uint32_t marshall_base<service::get_file_size_response>(::apache::thrift::protocol::TProtocol* oprot, const service::get_file_size_response& val)
    {
        uint32_t xfer = 0;
        oprot->incrementInputRecursionDepth();
        xfer += oprot->writeStructBegin("rpc_message");
        xfer += oprot->writeFieldBegin("msg", ::apache::thrift::protocol::T_STRUCT, 1);

        xfer += val.write(oprot);

        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        oprot->decrementInputRecursionDepth();
        return xfer;
    }

    template<>
    inline uint32_t unmarshall_base<service::get_file_size_response>(::apache::thrift::protocol::TProtocol* iprot, /*out*/ service::get_file_size_response& val)
    {
        uint32_t xfer = 0;
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;
        xfer += iprot->readStructBegin(fname);
        using ::apache::thrift::protocol::TProtocolException;
        while (true)
        {
            xfer += iprot->readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }
            switch (fid)
            {
            case 1:
                if (ftype == ::apache::thrift::protocol::T_STRUCT) {
                    xfer += val.read(iprot);
                }
                else {
                    xfer += iprot->skip(ftype);
                }
                break;
            default:
                xfer += iprot->skip(ftype);
                break;
            }
            xfer += iprot->readFieldEnd();
        }
        xfer += iprot->readStructEnd();
        iprot->readMessageEnd();
        iprot->getTransport()->readEnd();
        return xfer;
    }

}

namespace dsn { namespace service { 
    // ---------- copy_request -------------
    inline void marshall(::dsn::binary_writer& writer, const copy_request& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_base<copy_request>(&proto, val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ copy_request& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_base<copy_request>(&proto, val);
    }

    // ---------- copy_response -------------
    inline void marshall(::dsn::binary_writer& writer, const copy_response& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_base<copy_response>(&proto, val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ copy_response& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_base<copy_response>(&proto, val);
    }

    // ---------- get_file_size_request -------------
    inline void marshall(::dsn::binary_writer& writer, const get_file_size_request& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_base<get_file_size_request>(&proto, val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ get_file_size_request& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_base<get_file_size_request>(&proto, val);
    }

    // ---------- get_file_size_response -------------
    inline void marshall(::dsn::binary_writer& writer, const get_file_size_response& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_base<get_file_size_response>(&proto, val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ get_file_size_response& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_base<get_file_size_response>(&proto, val);
    }

} } 


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
