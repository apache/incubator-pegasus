include "../../dsn.thrift"
include "../../dsn.layer2.thrift"

namespace cpp dsn.replication

struct mutation_header
{
    1:dsn.gpid             pid;
    2:i64                  ballot;
    3:i64                  decree;
    4:i64                  log_offset;
    5:i64                  last_committed_decree;
}

struct mutation_update
{
    1:dsn.task_code  code;
    2:dsn.blob       data;
}

struct mutation_data
{
    1:mutation_header        header;
    2:list<mutation_update>  updates;
}

enum partition_status
{
    PS_INVALID,
    PS_INACTIVE,
    PS_ERROR,
    PS_PRIMARY,
    PS_SECONDARY,
    PS_POTENTIAL_SECONDARY,
}

struct replica_configuration
{
    1:dsn.gpid            pid;
    2:i64                 ballot;
    3:dsn.rpc_address     primary;
    4:partition_status    status = partition_status.PS_INVALID;
    5:i64                 learner_signature;
}

struct prepare_msg
{
    1:replica_configuration config;
    2:mutation_data         mu;
}

enum read_semantic
{
    ReadInvalid,
    ReadLastUpdate,
    ReadOutdated,
    ReadSnapshot,
}

struct read_request_header
{
    1:dsn.gpid pid;
    2:dsn.task_code       code;
    3:read_semantic       semantic = read_semantic.ReadLastUpdate;
    4:i64                 version_decree = -1;
}

struct write_request_header
{
    1:dsn.gpid pid;
    2:dsn.task_code       code;
}

struct rw_response_header
{
    1:dsn.error_code      err;
}

struct prepare_ack
{
    1:dsn.gpid pid;
    2:dsn.error_code      err;
    3:i64                 ballot;
    4:i64                 decree;
    5:i64                 last_committed_decree_in_app;
    6:i64                 last_committed_decree_in_prepare_list;
}

enum learn_type
{
    LT_INVALID,
    LT_CACHE,
    LT_APP,
    LT_LOG,
}

struct learn_state
{
    1:i64            from_decree_excluded;
    2:i64            to_decree_included;
    3:dsn.blob       meta;
    4:list<string>   files;
}

enum learner_status
{
    LearningInvalid,
    LearningWithoutPrepare,
    LearningWithPrepareTransient,
    LearningWithPrepare,
    LearningSucceeded,
    LearningFailed,
}

struct learn_request
{
    1:dsn.gpid pid;
    2:dsn.rpc_address     learner; // learner's address
    3:i64                 signature; // learning signature
    4:i64                 last_committed_decree_in_app; // last committed decree of learner's app
    5:i64                 last_committed_decree_in_prepare_list; // last committed decree of learner's prepare list
    6:dsn.blob            app_specific_learn_request; // learning request data by app.prepare_learn_request()
}

struct learn_response
{
    1:dsn.error_code        err; // error code
    2:replica_configuration config; // learner's replica config
    3:i64                   last_committed_decree; // learnee's last committed decree
    4:i64                   prepare_start_decree; // prepare start decree
    5:learn_type            type = learn_type.LT_INVALID; // learning type: CACHE, LOG, APP
    6:learn_state           state; // learning data, including memory data and files
    7:dsn.rpc_address       address; // learnee's address
    8:string                base_local_dir; // base dir of files on learnee
}

struct group_check_request
{
    1:dsn.layer2.app_info          app;
    2:dsn.rpc_address       node;
    3:replica_configuration config;
    4:i64                   last_committed_decree;
}

struct group_check_response
{
    1:dsn.gpid pid;
    2:dsn.error_code      err;
    3:i64                 last_committed_decree_in_app;
    4:i64                 last_committed_decree_in_prepare_list;
    5:learner_status      learner_status_ = learner_status.LearningInvalid;
    6:i64                 learner_signature;
    7:dsn.rpc_address     node;
}

/////////////////// meta server messages ////////////////////
enum config_type
{
    CT_INVALID,
    CT_ASSIGN_PRIMARY,
    CT_UPGRADE_TO_PRIMARY,
    CT_ADD_SECONDARY,
    CT_UPGRADE_TO_SECONDARY, // not used by meta server
    CT_DOWNGRADE_TO_SECONDARY,
    CT_DOWNGRADE_TO_INACTIVE,
    CT_REMOVE,
    CT_ADD_SECONDARY_FOR_LB,
}

enum node_status
{
    NS_INVALID,
    NS_ALIVE,
    NS_UNALIVE,
}

struct node_info
{
    1:node_status      status = node_status.NS_INVALID;
    2:dsn.rpc_address  address;
}

struct meta_response_header
{
    1:dsn.error_code   err;
    2:dsn.rpc_address  primary_address;
}

// primary | secondary(upgrading) (w/ new config) => meta server
// also served as proposals from meta server to replica servers
struct configuration_update_request
{
    1:dsn.layer2.app_info                 info;
    2:dsn.layer2.partition_configuration  config;
    3:config_type              type = config_type.CT_INVALID;
    4:dsn.rpc_address          node;
    5:dsn.rpc_address          host_node; // only used by stateless apps    
}

// meta server (config mgr) => primary | secondary (downgrade) (w/ new config)
struct configuration_update_response
{
    1:dsn.error_code           err;
    2:dsn.layer2.partition_configuration  config;
}

// client => meta server
struct configuration_query_by_node_request
{
    1:dsn.rpc_address  node;
}

struct configuration_query_by_node_response
{
    1:dsn.error_code                    err;
    2:list<configuration_update_request> partitions;
}

struct create_app_options
{
    1:i32              partition_count;
    2:i32              replica_count;
    3:bool             success_if_exist;
    4:string           app_type;
    5:bool             is_stateful;
    6:map<string, string>  envs;
}

struct configuration_create_app_request
{
    1:string                   app_name;
    2:create_app_options       options;
}

struct drop_app_options
{
    1:bool             success_if_not_exist;
}

struct configuration_drop_app_request
{
    1:string                   app_name;
    2:drop_app_options         options;
}

struct configuration_list_apps_request
{
    1:dsn.layer2.app_status    status = app_status.AS_INVALID;
}

struct configuration_list_nodes_request
{
    1:node_status              status = node_status.NS_INVALID;
}

// meta server => client
struct configuration_create_app_response
{
    1:dsn.error_code   err;
    2:i32              appid;
}

// load balancer control
struct control_balancer_migration_request
{
    1:bool             enable_migration;
}

struct control_balancer_migration_response
{
    1:dsn.error_code   err;
}

enum balancer_type
{
    BT_INVALID,
    BT_MOVE_PRIMARY,
    BT_COPY_PRIMARY,
    BT_COPY_SECONDARY
}

struct balancer_proposal_request
{
    1:dsn.gpid pid;
    2:balancer_type       type;
    3:dsn.rpc_address     from_addr;
    4:dsn.rpc_address     to_addr;
}

struct balancer_proposal_response
{
    1:dsn.error_code   err;
}

struct configuration_drop_app_response
{
    1:dsn.error_code   err;
}

struct configuration_list_apps_response
{
    1:dsn.error_code              err;
    2:list<dsn.layer2.app_info>   infos;
}

struct configuration_list_nodes_response
{
    1:dsn.error_code   err;
    2:list<node_info>  infos;
}

struct query_replica_decree_request
{
    1:dsn.gpid pid;
    2:dsn.rpc_address     node;
}

struct query_replica_decree_response
{
    1:dsn.error_code      err;
    2:i64                 last_decree;
}

struct replica_info
{
    1:dsn.gpid               pid;
    2:i64                    ballot;
    3:partition_status       status;
    4:i64                    last_committed_decree;
    5:i64                    last_prepared_decree;
    6:i64                    last_durable_decree;
}

struct query_replica_info_request
{
    1:dsn.rpc_address     node;
}

struct query_replica_info_response
{
    1:dsn.error_code      err;
    2:list<replica_info>  replicas;
}

struct app_state
{
    1:dsn.layer2.app_info info;
    2:dsn.atom_int available_partitions;
    3:list< dsn.layer2.partition_configuration> partitions;
}

struct node_state
{
    1:bool            is_alive;
    2:dsn.rpc_address address;
    3:set<dsn.gpid>   primaries;
    4:set<dsn.gpid>   partitions;
}

service meta_s
{
    configuration_create_app_response create_app(1:configuration_create_app_request req);
    configuration_drop_app_response drop_app(1:configuration_drop_app_request req);
    configuration_list_nodes_response list_nodes(1:configuration_list_nodes_request req);
    configuration_list_apps_response list_apps(1:configuration_list_apps_request req);
    configuration_query_by_node_response query_configuration_by_node(1:configuration_query_by_node_request query);
    configuration_query_by_index_response query_configuration_by_index(1:configuration_query_by_index_request query);
    
}
/*
service replica_s
{
    rw_response_header client_write(1:write_request_header req);
    rw_response_header client_read(1:read_request_header req);
    prepare_ack prepare(1:prepare_msg request);
    void config_proposal(1:configuration_update_request proposal);
    learn_response learn(1:learn_request request);
    void learn_completion_notification(1:group_check_response report);
    void add_learner(1:group_check_request request);
    void remove(1:replica_configuration request);
    group_check_response group_check(1:group_check_request request);
    query_replica_decree_response query_decree(1:query_replica_decree_request req);
    query_replica_info_response query_replica_info(1:query_replica_info_request req);
}

service meta_s
{
    configuration_create_app_response create_app(1:configuration_create_app_request req);
    configuration_drop_app_response drop_app(1:configuration_drop_app_request req);
    configuration_query_by_node_response query_configuration_by_node(1:configuration_query_by_node_request query);
    configuration_query_by_index_response query_configuration_by_index(1:configuration_query_by_index_request query);
    configuration_update_response update_configuration(1:configuration_update_request update);
}
*/
