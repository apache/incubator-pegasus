
include "../../dsn.thrift"

namespace cpp dsn.replication

struct global_partition_id
{
    1:i32  app_id = -1;
    2:i32  pidx = -1;
}

struct mutation_header
{
    1:global_partition_id gpid;
    2:i64             ballot;
    3:i64             decree;
    4:i64             log_offset;
    5:i64             last_committed_decree;
}

struct mutation_data
{
    1:mutation_header header;
    2:list<dsn.blob>  updates;
}

enum partition_status
{
    PS_INACTIVE,
    PS_ERROR,
    PS_PRIMARY,
    PS_SECONDARY,
    PS_POTENTIAL_SECONDARY,
    PS_INVALID,
}

struct partition_configuration
{
    1:string                 app_type;
    2:global_partition_id    gpid;
    3:i64                    ballot;
    4:i32                    max_replica_count;
    5:dsn.address            primary;
    6:list<dsn.address>      secondaries;
    7:list<dsn.address>      last_drops;
    8:i64                    last_committed_decree;
}

struct replica_configuration
{
    1:global_partition_id gpid;
    2:i64                 ballot;
    3:dsn.address         primary;
    4:partition_status    status = partition_status.PS_INACTIVE;
}

struct prepare_msg
{
    1:replica_configuration config;
    2:mutation_data         mu;
}

enum read_semantic_t
{
    ReadLastUpdate,
    ReadOutdated,
    ReadSnapshot
}

struct read_request_header
{
    1:global_partition_id gpid;
    2:string              code;
    3:read_semantic_t     semantic = read_semantic_t.ReadLastUpdate;
    4:i64                 version_decree = -1;
}

struct write_request_header
{
    1:global_partition_id gpid;
    2:string              code;
}

struct rw_response_header
{
    1:dsn.error_code     err;
}

struct prepare_ack
{
    1:global_partition_id gpid;
    2:dsn.error_code      err;
    3:i64                 ballot;
    4:i64                 decree;
    5:i64                 last_committed_decree_in_app;
    6:i64                 last_committed_decree_in_prepare_list;
}

enum learn_type
{
    LT_NONE,
    LT_CACHE,
    LT_APP,
    LT_LOG,
}

struct learn_state
{
    1:list<dsn.blob> meta;
    2:list<string>   files;
}

enum learner_status
{
    LearningWithoutPrepare,
    LearningWithPrepare,
    LearningWithPrepareS2, // prepare + checkpointing
    LearningSucceeded,
    LearningFailed,
    Learning_INVALID
}

struct learn_request
{
    1:global_partition_id gpid;
    2:dsn.address         learner;
    3:i64                 signature;
    4:i64                 last_committed_decree_in_app;
    5:i64                 last_committed_decree_in_prepare_list;
    6:dsn.blob            app_specific_learn_request;
}

struct learn_response
{
    1:dsn.error_code        err;
    2:replica_configuration config;
    3:i64                   commit_decree;
    4:i64                   prepare_start_decree;
    5:learn_type            type;
    6:learn_state           state;
    7:string                base_local_dir;
}

struct group_check_request
{
    1:string                app_type;
    2:dsn.address           node;
    3:replica_configuration config;
    4:i64                   last_committed_decree;
    5:i64                   learner_signature;
}

struct group_check_response
{
    1:global_partition_id gpid;
    2:dsn.error_code      err;
    3:i64                 last_committed_decree_in_app;
    4:i64                 last_committed_decree_in_prepare_list;
    5:learner_status      learner_status_ = learner_status.LearningFailed;
    6:i64                 learner_signature;
    7:dsn.address         node;
}

/////////////////// meta server messages ////////////////////
enum config_type
{
    CT_NONE,
    CT_ASSIGN_PRIMARY,
    CT_UPGRADE_TO_PRIMARY,
    CT_ADD_SECONDARY,
    CT_DOWNGRADE_TO_SECONDARY,
    CT_DOWNGRADE_TO_INACTIVE,
    CT_REMOVE,

    // not used by meta server
    CT_UPGRADE_TO_SECONDARY,
}

struct meta_request_header
{
    1:string rpc_tag;
}

struct meta_response_header
{
    1:dsn.error_code err;
    2:dsn.address  primary_address;
}

// primary | secondary(upgrading) (w/ new config) => meta server
struct configuration_update_request
{
    1:partition_configuration  config;
    2:config_type              type = config_type.CT_NONE;
    3:dsn.address              node;
}

// meta server (config mgr) => primary | secondary (downgrade) (w/ new config)
struct configuration_update_response
{
    1:dsn.error_code           err;
    2:partition_configuration  config;
}

// proposal:  meta server(LBM) => primary  (w/ current config)
struct configuration_proposal_request
{
    1:partition_configuration  config;
    2:config_type              type = config_type.CT_NONE;
    3:dsn.address              node;
    4:bool                     is_clean_data = false;
    5:bool                     is_upgrade = false;
}

// client => meta server
struct configuration_query_by_node_request
{
    1:dsn.address    node;
}

// meta server => client
struct configuration_query_by_node_response
{
    1:dsn.error_code                err;
    2:list<partition_configuration> partitions;
}

struct configuration_query_by_index_request
{
    1:string           app_name;
    2:list<i32>        partition_indices;
}

struct configuration_query_by_index_response
{
    1:dsn.error_code                err;
    2:i32                           app_id;
    3:i32                           partition_count;
    4:list<partition_configuration> partitions;
}

struct query_replica_decree_request
{
    1:global_partition_id gpid;
    2:dsn.address         node;
}

struct query_replica_decree_response
{
    1:dsn.error_code      err;
    2:i64                 last_decree;
}

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
}

service meta_s
{
    configuration_query_by_node_response query_configuration_by_node(1:configuration_query_by_node_request query);
    configuration_query_by_index_response query_configuration_by_index(1:configuration_query_by_index_request query);
    configuration_update_response update_configuration(1:configuration_update_request update);
}
