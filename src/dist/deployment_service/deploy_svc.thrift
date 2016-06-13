include "../../dsn.thrift"
include "../../dsn.layer2.thrift"

namespace cpp dsn.dist

enum cluster_type
{
    cstype_kubernetes,
    cstype_docker,
    cstype_bare_medal_linux,
    cstype_bare_medal_windows,
    cstype_yarn_on_linux,
    cstype_yarn_on_windows,
    cstype_mesos_on_linux,
    cstype_mesos_on_windows,
    cstype_count,
    cstype_invalid
}

enum service_status
{
    SS_PREPARE_RESOURCE,
    SS_DEPLOYING,
    SS_RUNNING,
    SS_FAILOVER,
    SS_FAILED,
    SS_UNDEPLOYING,
    SS_UNDEPLOYED,
    SS_COUNT,
    SS_INVALID
}

struct deploy_request
{
    1:dsn.layer2.app_info info;
    2:string package_full_path;
    3:dsn.rpc_address package_server;
    4:string cluster_name;
    5:string name;
    // TODO: other information ...
}

struct deploy_info
{
    1:dsn.layer2.app_info info;
    2:string name;
    3:string service_url;
    4:dsn.error_code error;
    5:string cluster;
    6:service_status status;
    // TODO: more info
}

struct deploy_info_list
{
    1:list<deploy_info> services;
}

struct cluster_info
{
    1:string name;
    2:cluster_type type;
}

struct cluster_list
{
    1:list<cluster_info> clusters;
}

service deploy_svc
{
    //
    // service deployments
    //
    deploy_info deploy(1:deploy_request req);    
    // return error code
    dsn.error_code undeploy(1:string service_url);    
    deploy_info_list get_service_list(1:string app_type);
    deploy_info get_service_info(1:string service_url);    
    
    //
    // cluster information
    //
    cluster_list get_cluster_list(1:string format);
}
