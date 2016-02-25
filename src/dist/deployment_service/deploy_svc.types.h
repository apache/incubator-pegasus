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


# else // use rDSN's data encoding/decoding

#include <dsn/dist/cluster_scheduler.h>
namespace dsn { namespace dist { 

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
