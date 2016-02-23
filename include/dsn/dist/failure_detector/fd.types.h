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

namespace dsn {
    // ---------- beacon_msg -------------
    template<>
    inline uint32_t marshall_base< ::dsn::fd::beacon_msg>(::apache::thrift::protocol::TProtocol* oprot, const ::dsn::fd::beacon_msg& val)
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
    inline uint32_t unmarshall_base< ::dsn::fd::beacon_msg>(::apache::thrift::protocol::TProtocol* iprot, /*out*/ ::dsn::fd::beacon_msg& val)
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

    // ---------- beacon_ack -------------
    template<>
    inline uint32_t marshall_base< ::dsn::fd::beacon_ack>(::apache::thrift::protocol::TProtocol* oprot, const ::dsn::fd::beacon_ack& val)
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
    inline uint32_t unmarshall_base< ::dsn::fd::beacon_ack>(::apache::thrift::protocol::TProtocol* iprot, /*out*/ ::dsn::fd::beacon_ack& val)
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

namespace dsn { namespace fd { 
    // ---------- beacon_msg -------------
    inline void marshall(::dsn::binary_writer& writer, const beacon_msg& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_base<beacon_msg>(&proto, val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ beacon_msg& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_base<beacon_msg>(&proto, val);
    }

    // ---------- beacon_ack -------------
    inline void marshall(::dsn::binary_writer& writer, const beacon_ack& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_base<beacon_ack>(&proto, val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ beacon_ack& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_base<beacon_ack>(&proto, val);
    }

} } 


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
