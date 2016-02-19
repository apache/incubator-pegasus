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
# include "deploy_svc_types.h" 

namespace dsn {
    // ---------- deploy_request -------------
    template<>
    inline uint32_t marshall_base< ::dsn::dist::deploy_request>(::apache::thrift::protocol::TProtocol* oprot, const ::dsn::dist::deploy_request& val)
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
    inline uint32_t unmarshall_base< ::dsn::dist::deploy_request>(::apache::thrift::protocol::TProtocol* iprot, /*out*/ ::dsn::dist::deploy_request& val)
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

    // ---------- deploy_info -------------
    template<>
    inline uint32_t marshall_base< ::dsn::dist::deploy_info>(::apache::thrift::protocol::TProtocol* oprot, const ::dsn::dist::deploy_info& val)
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
    inline uint32_t unmarshall_base< ::dsn::dist::deploy_info>(::apache::thrift::protocol::TProtocol* iprot, /*out*/ ::dsn::dist::deploy_info& val)
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

    // ---------- deploy_info_list -------------
    template<>
    inline uint32_t marshall_base< ::dsn::dist::deploy_info_list>(::apache::thrift::protocol::TProtocol* oprot, const ::dsn::dist::deploy_info_list& val)
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
    inline uint32_t unmarshall_base< ::dsn::dist::deploy_info_list>(::apache::thrift::protocol::TProtocol* iprot, /*out*/ ::dsn::dist::deploy_info_list& val)
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

    // ---------- cluster_info -------------
    template<>
    inline uint32_t marshall_base< ::dsn::dist::cluster_info>(::apache::thrift::protocol::TProtocol* oprot, const ::dsn::dist::cluster_info& val)
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
    inline uint32_t unmarshall_base< ::dsn::dist::cluster_info>(::apache::thrift::protocol::TProtocol* iprot, /*out*/ ::dsn::dist::cluster_info& val)
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

    // ---------- cluster_list -------------
    template<>
    inline uint32_t marshall_base< ::dsn::dist::cluster_list>(::apache::thrift::protocol::TProtocol* oprot, const ::dsn::dist::cluster_list& val)
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
    inline uint32_t unmarshall_base< ::dsn::dist::cluster_list>(::apache::thrift::protocol::TProtocol* iprot, /*out*/ ::dsn::dist::cluster_list& val)
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

namespace dsn { namespace dist { 
    // ---------- deploy_request -------------
    inline void marshall(::dsn::binary_writer& writer, const deploy_request& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_base<deploy_request>(&proto, val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ deploy_request& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_base<deploy_request>(&proto, val);
    }

    // ---------- deploy_info -------------
    inline void marshall(::dsn::binary_writer& writer, const deploy_info& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_base<deploy_info>(&proto, val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ deploy_info& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_base<deploy_info>(&proto, val);
    }

    // ---------- deploy_info_list -------------
    inline void marshall(::dsn::binary_writer& writer, const deploy_info_list& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_base<deploy_info_list>(&proto, val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ deploy_info_list& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_base<deploy_info_list>(&proto, val);
    }

    // ---------- cluster_info -------------
    inline void marshall(::dsn::binary_writer& writer, const cluster_info& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_base<cluster_info>(&proto, val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ cluster_info& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_base<cluster_info>(&proto, val);
    }

    // ---------- cluster_list -------------
    inline void marshall(::dsn::binary_writer& writer, const cluster_list& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_base<cluster_list>(&proto, val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ cluster_list& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_base<cluster_list>(&proto, val);
    }

} } 


# else // use rDSN's data encoding/decoding

namespace dsn { namespace dist { 
    // ---------- cluster_type -------------
    enum cluster_type
    {
        kubernetes = 0,
        docker = 1,
        bare_medal_linux = 2,
        bare_medal_windows = 3,
        yarn_on_linux = 4,
        yarn_on_windows = 5,
        mesos_on_linux = 6,
        mesos_on_windows = 7,
    };

    DEFINE_POD_SERIALIZATION(cluster_type);

    // ---------- service_status -------------
    enum service_status
    {
        SS_PREPARE_RESOURCE = 0,
        SS_DEPLOYING = 1,
        SS_RUNNING = 2,
        SS_FAILOVER = 3,
        SS_FAILED = 4,
        SS_COUNT = 5,
        SS_INVALID = 6,
    };

    DEFINE_POD_SERIALIZATION(service_status);

    // ---------- deploy_request -------------
    struct deploy_request
    {
        std::string package_id;
        std::string package_full_path;
        ::dsn::rpc_address package_server;
        std::string cluster_name;
        std::string name;
    };

    inline void marshall(::dsn::binary_writer& writer, const deploy_request& val)
    {
        marshall(writer, val.package_id);
        marshall(writer, val.package_full_path);
        marshall(writer, val.package_server);
        marshall(writer, val.cluster_name);
        marshall(writer, val.name);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ deploy_request& val)
    {
        unmarshall(reader, val.package_id);
        unmarshall(reader, val.package_full_path);
        unmarshall(reader, val.package_server);
        unmarshall(reader, val.cluster_name);
        unmarshall(reader, val.name);
    }

    // ---------- deploy_info -------------
    struct deploy_info
    {
        std::string package_id;
        std::string name;
        std::string service_url;
        ::dsn::error_code error;
        std::string cluster;
        service_status status;
    };

    inline void marshall(::dsn::binary_writer& writer, const deploy_info& val)
    {
        marshall(writer, val.package_id);
        marshall(writer, val.name);
        marshall(writer, val.service_url);
        marshall(writer, val.error);
        marshall(writer, val.cluster);
        marshall(writer, val.status);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ deploy_info& val)
    {
        unmarshall(reader, val.package_id);
        unmarshall(reader, val.name);
        unmarshall(reader, val.service_url);
        unmarshall(reader, val.error);
        unmarshall(reader, val.cluster);
        unmarshall(reader, val.status);
    }

    // ---------- deploy_info_list -------------
    struct deploy_info_list
    {
        std::vector< deploy_info> services;
    };

    inline void marshall(::dsn::binary_writer& writer, const deploy_info_list& val)
    {
        marshall(writer, val.services);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ deploy_info_list& val)
    {
        unmarshall(reader, val.services);
    }

    // ---------- cluster_info -------------
    struct cluster_info
    {
        std::string name;
        cluster_type type;
    };

    inline void marshall(::dsn::binary_writer& writer, const cluster_info& val)
    {
        marshall(writer, val.name);
        marshall(writer, val.type);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ cluster_info& val)
    {
        unmarshall(reader, val.name);
        unmarshall(reader, val.type);
    }

    // ---------- cluster_list -------------
    struct cluster_list
    {
        std::vector< cluster_info> clusters;
    };

    inline void marshall(::dsn::binary_writer& writer, const cluster_list& val)
    {
        marshall(writer, val.clusters);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ cluster_list& val)
    {
        unmarshall(reader, val.clusters);
    }

} } 

#endif 
