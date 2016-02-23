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

namespace dsn {
    // ---------- kv_pair -------------
    template<>
    inline uint32_t marshall_base< ::dsn::replication::test::kv_pair>(::apache::thrift::protocol::TProtocol* oprot, const ::dsn::replication::test::kv_pair& val)
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
    inline uint32_t unmarshall_base< ::dsn::replication::test::kv_pair>(::apache::thrift::protocol::TProtocol* iprot, /*out*/ ::dsn::replication::test::kv_pair& val)
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

namespace dsn { namespace replication { namespace test { 
    // ---------- kv_pair -------------
    inline void marshall(::dsn::binary_writer& writer, const kv_pair& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_base<kv_pair>(&proto, val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ kv_pair& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_base<kv_pair>(&proto, val);
    }

} } } 


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
