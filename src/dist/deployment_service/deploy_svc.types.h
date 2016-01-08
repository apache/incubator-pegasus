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


    // ---------- deploy_request -------------
    inline void marshall(::dsn::binary_writer& writer, const deploy_request& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<deploy_request>(&proto, val, &deploy_request::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ deploy_request& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<deploy_request>(&proto, val, &deploy_request::read);
    };

    // ---------- deploy_info -------------
    inline void marshall(::dsn::binary_writer& writer, const deploy_info& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<deploy_info>(&proto, val, &deploy_info::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ deploy_info& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<deploy_info>(&proto, val, &deploy_info::read);
    };

    // ---------- deploy_info_list -------------
    inline void marshall(::dsn::binary_writer& writer, const deploy_info_list& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<deploy_info_list>(&proto, val, &deploy_info_list::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ deploy_info_list& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<deploy_info_list>(&proto, val, &deploy_info_list::read);
    };

    // ---------- cluster_info -------------
    inline void marshall(::dsn::binary_writer& writer, const cluster_info& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<cluster_info>(&proto, val, &cluster_info::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ cluster_info& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<cluster_info>(&proto, val, &cluster_info::read);
    };

    // ---------- cluster_list -------------
    inline void marshall(::dsn::binary_writer& writer, const cluster_list& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<cluster_list>(&proto, val, &cluster_list::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ cluster_list& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<cluster_list>(&proto, val, &cluster_list::read);
    };




# else // use rDSN's data encoding/decoding


    // ---------- deploy_request -------------
    struct deploy_request
    {
        std::string package_id;
        std::string package_url;
        std::string cluster_name;
        std::string name;
    };

    inline void marshall(::dsn::binary_writer& writer, const deploy_request& val)
    {
        marshall(writer, val.package_id);
        marshall(writer, val.package_url);
        marshall(writer, val.cluster_name);
        marshall(writer, val.name);
    };

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ deploy_request& val)
    {
        unmarshall(reader, val.package_id);
        unmarshall(reader, val.package_url);
        unmarshall(reader, val.cluster_name);
        unmarshall(reader, val.name);
    };

    // ---------- deploy_info -------------
    struct deploy_info
    {
        std::string package_id;
        std::string name;
        std::string error;
        std::string cluster;
        std::string dsptr;
    };

    inline void marshall(::dsn::binary_writer& writer, const deploy_info& val)
    {
        marshall(writer, val.package_id);
        marshall(writer, val.name);
        marshall(writer, val.error);
        marshall(writer, val.cluster);
        marshall(writer, val.dsptr);
    };

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ deploy_info& val)
    {
        unmarshall(reader, val.package_id);
        unmarshall(reader, val.name);
        unmarshall(reader, val.error);
        unmarshall(reader, val.cluster);
        unmarshall(reader, val.dsptr);
    };

    // ---------- deploy_info_list -------------
    struct deploy_info_list
    {
        std::vector< deploy_info> services;
    };

    inline void marshall(::dsn::binary_writer& writer, const deploy_info_list& val)
    {
        marshall(writer, val.services);
    };

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ deploy_info_list& val)
    {
        unmarshall(reader, val.services);
    };

    // ---------- cluster_info -------------
    struct cluster_info
    {
        std::string name;
        std::string type;
    };

    inline void marshall(::dsn::binary_writer& writer, const cluster_info& val)
    {
        marshall(writer, val.name);
        marshall(writer, val.type);
    };

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ cluster_info& val)
    {
        unmarshall(reader, val.name);
        unmarshall(reader, val.type);
    };

    // ---------- cluster_list -------------
    struct cluster_list
    {
        std::vector< cluster_info> clusters;
    };

    inline void marshall(::dsn::binary_writer& writer, const cluster_list& val)
    {
        marshall(writer, val.clusters);
    };

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ cluster_list& val)
    {
        unmarshall(reader, val.clusters);
    };



#endif 
