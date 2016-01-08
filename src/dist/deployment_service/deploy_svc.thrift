
struct deploy_request
{
    1:string package_id;
    2:string package_url;
    3:string cluster_name;
    4:string name;
    // TODO: other information ...
}

struct deploy_info
{
    1:string package_id;
    2:string name;
    3:string error;
    4:string cluster;
    5:string dsptr;
    // TODO: more info
}

struct deploy_info_list
{
    1:list<deploy_info> services;
}

struct cluster_info
{
    1:string name;
    2:string type;
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
    // return error code to string
    string undeploy(1:string service_url);    
    deploy_info_list get_service_list(1:string package_id);
    deploy_info get_service_info(1:string service_url);    
    
    //
    // cluster information
    //
    cluster_list get_cluster_list(1:string format);
}
