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
    6:i64                  timestamp;
}

struct mutation_update
{
    1:dsn.task_code  code;

    //the serialization type of data, this need to store in log and replicate to secondaries by primary
    2:i32            serialization_type;
    3:dsn.blob       data;
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
    PS_POTENTIAL_SECONDARY
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

struct learn_notify_response
{
    1:dsn.gpid pid;
    2:dsn.error_code        err; // error code
    3:i64                   signature; // learning signature
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
    CT_PRIMARY_FORCE_UPDATE_BALLOT,
    CT_DROP_PARTITION
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
struct replica_server_info
{
    // replica server can report its geo position
    // possible tags may be:
    // geo_tags["host"] = hostid;
    // geo_tags["rack"] = rackid
    // geo_tags["datacenter"] = datacenterid
    // geo_tags["city"] = cityid
    1:map<string, string> geo_tags;
    2:i64 total_capacity_mb;
}

struct configuration_query_by_node_request
{
    1:dsn.rpc_address  node;
    2:optional list<replica_info> stored_replicas;
    3:optional replica_server_info info;
}

struct configuration_query_by_node_response
{
    1:dsn.error_code err;
    2:list<configuration_update_request> partitions;
    3:optional list<replica_info> gc_replicas;
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
    2:optional i64     reserve_seconds;
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

struct configuration_cluster_info_request
{
}

struct configuration_recall_app_request
{
    1:i32 app_id;
    2:string new_app_name;
}

// meta server => client
struct configuration_create_app_response
{
    1:dsn.error_code   err;
    2:i32              appid;
}

enum meta_function_level
{
    // there are 4 ways to modify the meta-server's status:
    // 0. DDL operation: create/drop/recall table
    // 1. downgrade primary when dectect it is not alive
    // 2. accept primary's update-request to kickoff some secondaries
    // 3. make balancer proposal, which further trigger 2
    // according to these ways, we give meta several active level.

    fl_stopped = 100, //we don't take any action to modify the meta's status, even the DDL operations are not responsed
    fl_blind = 200, //only DDL operations are responsed, 1 2 3 are just ignored
    fl_freezed = 300, //0 1 are responsed, 2 3 ignored
    fl_steady = 400, //0 1 2 are responsed, don't do any balancer
    fl_lively = 500, //full functional
    fl_invalid = 10000
}

// if the level is invalid, we just response the old level of meta without updating it
struct configuration_meta_control_request
{
    1:meta_function_level level;
}

struct configuration_meta_control_response
{
    1:dsn.error_code err;
    2:meta_function_level old_level;
}

struct configuration_proposal_action
{
    1:dsn.rpc_address target;
    2:dsn.rpc_address node;
    3:config_type type;

    // depricated now
    // new fields of this struct should start with 5
    // 4:i64 period_ts;
}

struct configuration_balancer_request
{
    1:dsn.gpid gpid;
    2:list<configuration_proposal_action> action_list;
    3:optional bool force = false;
}

struct configuration_balancer_response
{
    1:dsn.error_code err;
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

struct configuration_cluster_info_response
{
    1:dsn.error_code   err;
    2:list<string>     keys;
    3:list<string>     values;
}

struct configuration_recall_app_response
{
    1:dsn.error_code err;
    2:dsn.layer2.app_info info;
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
    7:string                 app_type;
    8:string                 disk_tag;
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

struct query_app_info_request
{
    1:dsn.rpc_address meta_server;
}

struct query_app_info_response
{
    1:dsn.error_code err;
    2:list<dsn.layer2.app_info> apps;
}

struct configuration_recovery_request
{
    1:list<dsn.rpc_address> recovery_set;
    2:bool skip_bad_nodes;
    3:bool skip_lost_partitions;
}

struct configuration_recovery_response
{
    1:dsn.error_code err;
    2:string hint_message;
}

struct policy_info
{
    1:string        policy_name;
    2:string        backup_provider_type;
}

// using configuration_create_app_response to response
struct configuration_restore_request
{
    1:string            cluster_name;
    2:string            policy_name;
    3:i64               time_stamp;   // namely backup_id
    4:string            app_name;
    5:i32               app_id;
    6:string            new_app_name;
    7:string            backup_provider_name;
    8:bool              skip_bad_partition;
}

struct backup_request
{
    1:dsn.gpid              pid;
    2:policy_info           policy;
    3:string                app_name;
    4:i64                   backup_id;
}

struct backup_response
{
    1:dsn.error_code    err;
    2:dsn.gpid          pid;
    3:i32               progress;  // the progress of the cold_backup
    4:string            policy_name;
    5:i64               backup_id;
}

struct configuration_modify_backup_policy_request
{
    1:string                    policy_name;
    2:optional list<i32>        add_appids;
    3:optional list<i32>        removal_appids;
    4:optional i64              new_backup_interval_sec;
    5:optional i32              backup_history_count_to_keep;
    6:optional bool             is_disable;
    7:optional string           start_time; // restrict the start time of each backup, hour:minute
}

struct configuration_modify_backup_policy_response
{
    1:dsn.error_code        err;
    2:string                hint_message;
}

struct configuration_add_backup_policy_request
{
    1:string            backup_provider_type;
    2:string            policy_name;
    3:list<i32>         app_ids;
    4:i64               backup_interval_seconds;
    5:i32               backup_history_count_to_keep;
    6:string            start_time;
}

struct configuration_add_backup_policy_response
{
    1:dsn.error_code        err;
    2:string                hint_message;
}

struct policy_entry
{
    1:string        policy_name;
    2:string        backup_provider_type;
    3:string        backup_interval_seconds;
    4:set<i32>      app_ids;
    5:i32           backup_history_count_to_keep;
    6:string        start_time;
    7:bool          is_disable;
}

struct backup_entry
{
    1:i64           backup_id;
    2:i64           start_time_ms;
    3:i64           end_time_ms;
    4:set<i32>      app_ids;
}

struct configuration_query_backup_policy_request
{
    1:list<string>      policy_names;
    2:i32               backup_info_count;
}

struct configuration_query_backup_policy_response
{
    1:dsn.error_code            err;
    2:list<policy_entry>        policys;
    3:list<list<backup_entry>>  backup_infos;
    4:optional string           hint_msg;
}

struct configuration_report_restore_status_request
{
    1:dsn.gpid  pid;
    2:dsn.error_code    restore_status;
    3:i32        progress; //[0~1000]
    4:optional string   reason;
}

struct configuration_report_restore_status_response
{
    1:dsn.error_code    err;
}

struct configuration_query_restore_request
{
    1:i32   restore_app_id;
}

struct configuration_query_restore_response
{
    1:dsn.error_code        err;
    2:list<dsn.error_code>  restore_status;
    3:list<i32>             restore_progress;
}

/*
service replica_s
{
    rw_response_header client_write(1:write_request_header req);
    rw_response_header client_read(1:read_request_header req);
    prepare_ack prepare(1:prepare_msg request);
    void config_proposal(1:configuration_update_request proposal);
    learn_response learn(1:learn_request request);
    learn_notify_response learn_completion_notification(1:group_check_response report);
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
    configuration_recall_app_response recall_app(1:configuration_recall_app_request req);
    configuration_list_nodes_response list_nodes(1:configuration_list_nodes_request req);
    configuration_list_apps_response list_apps(1:configuration_list_apps_request req);

    configuration_query_by_node_response query_configuration_by_node(1:configuration_query_by_node_request query);
    configuration_query_by_index_response query_configuration_by_index(1:configuration_query_by_index_request query);
    configuration_sync_response config_sync(1:configuration_sync_request req);
    configuration_meta_control_response control_meta(1:configuration_meta_control_request req); // depreciated
    configuration_meta_control_response control_meta_level(1:configuration_meta_control_request req);
}
*/
