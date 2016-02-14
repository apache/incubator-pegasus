using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using dsn.dev.csharp;

namespace dsn.replication 
{
    using dsntask_code = TaskCode;
    using dsnrpc_address = RpcAddress;
    using dsnerror_code = Int32;
    using dsnblob = String;

    // ---------- partition_status -------------
    public enum partition_status
    {
        PS_INVALID = 0,
        PS_INACTIVE = 1,
        PS_ERROR = 2,
        PS_PRIMARY = 3,
        PS_SECONDARY = 4,
        PS_POTENTIAL_SECONDARY = 5,
    }

    // ---------- read_semantic -------------
    public enum read_semantic
    {
        ReadInvalid = 0,
        ReadLastUpdate = 1,
        ReadOutdated = 2,
        ReadSnapshot = 3,
    }

    // ---------- learn_type -------------
    public enum learn_type
    {
        LT_INVALID = 0,
        LT_CACHE = 1,
        LT_APP = 2,
        LT_LOG = 3,
    }

    // ---------- learner_status -------------
    public enum learner_status
    {
        LearningInvalid = 0,
        LearningWithoutPrepare = 1,
        LearningWithPrepareTransient = 2,
        LearningWithPrepare = 3,
        LearningSucceeded = 4,
        LearningFailed = 5,
    }

    // ---------- config_type -------------
    public enum config_type
    {
        CT_INVALID = 0,
        CT_ASSIGN_PRIMARY = 1,
        CT_UPGRADE_TO_PRIMARY = 2,
        CT_ADD_SECONDARY = 3,
        CT_UPGRADE_TO_SECONDARY = 4,
        CT_DOWNGRADE_TO_SECONDARY = 5,
        CT_DOWNGRADE_TO_INACTIVE = 6,
        CT_REMOVE = 7,
    }

    // ---------- app_status -------------
    public enum app_status
    {
        AS_INVALID = 0,
        AS_AVAILABLE = 1,
        AS_CREATING = 2,
        AS_CREATE_FAILED = 3,
        AS_DROPPING = 4,
        AS_DROP_FAILED = 5,
        AS_DROPPED = 6,
    }

    // ---------- node_status -------------
    public enum node_status
    {
        NS_INVALID = 0,
        NS_ALIVE = 1,
        NS_UNALIVE = 2,
    }

    // ---------- global_partition_id -------------
    public class global_partition_id
    {
        public Int32 app_id;
        public Int32 pidx;
    }

    // ---------- mutation_header -------------
    public class mutation_header
    {
        public global_partition_id gpid;
        public Int64 ballot;
        public Int64 decree;
        public Int64 log_offset;
        public Int64 last_committed_decree;
    }

    // ---------- mutation_update -------------
    public class mutation_update
    {
        public dsntask_code code;
        public dsnblob data;
    }

    // ---------- mutation_data -------------
    public class mutation_data
    {
        public mutation_header header;
        public List< mutation_update> updates;
    }

    // ---------- partition_configuration -------------
    public class partition_configuration
    {
        public string app_type;
        public global_partition_id gpid;
        public Int64 ballot;
        public Int32 max_replica_count;
        public dsnrpc_address primary;
        public List< dsnrpc_address> secondaries;
        public List< dsnrpc_address> last_drops;
        public Int64 last_committed_decree;
    }

    // ---------- replica_configuration -------------
    public class replica_configuration
    {
        public global_partition_id gpid;
        public Int64 ballot;
        public dsnrpc_address primary;
        public partition_status status;
        public Int64 learner_signature;
    }

    // ---------- prepare_msg -------------
    public class prepare_msg
    {
        public replica_configuration config;
        public mutation_data mu;
    }

    // ---------- read_request_header -------------
    public class read_request_header
    {
        public global_partition_id gpid;
        public dsntask_code code;
        public read_semantic semantic;
        public Int64 version_decree;
    }

    // ---------- write_request_header -------------
    public class write_request_header
    {
        public global_partition_id gpid;
        public dsntask_code code;
    }

    // ---------- rw_response_header -------------
    public class rw_response_header
    {
        public dsnerror_code err;
    }

    // ---------- prepare_ack -------------
    public class prepare_ack
    {
        public global_partition_id gpid;
        public dsnerror_code err;
        public Int64 ballot;
        public Int64 decree;
        public Int64 last_committed_decree_in_app;
        public Int64 last_committed_decree_in_prepare_list;
    }

    // ---------- learn_state -------------
    public class learn_state
    {
        public Int64 from_decree_excluded;
        public Int64 to_decree_included;
        public List< dsnblob> meta;
        public List< string> files;
    }

    // ---------- learn_request -------------
    public class learn_request
    {
        public global_partition_id gpid;
        public dsnrpc_address learner;
        public Int64 signature;
        public Int64 last_committed_decree_in_app;
        public Int64 last_committed_decree_in_prepare_list;
        public dsnblob app_specific_learn_request;
    }

    // ---------- learn_response -------------
    public class learn_response
    {
        public dsnerror_code err;
        public replica_configuration config;
        public Int64 last_committed_decree;
        public Int64 prepare_start_decree;
        public learn_type type;
        public learn_state state;
        public dsnrpc_address address;
        public string base_local_dir;
    }

    // ---------- group_check_request -------------
    public class group_check_request
    {
        public string app_type;
        public dsnrpc_address node;
        public replica_configuration config;
        public Int64 last_committed_decree;
    }

    // ---------- group_check_response -------------
    public class group_check_response
    {
        public global_partition_id gpid;
        public dsnerror_code err;
        public Int64 last_committed_decree_in_app;
        public Int64 last_committed_decree_in_prepare_list;
        public learner_status learner_status_;
        public Int64 learner_signature;
        public dsnrpc_address node;
    }

    // ---------- app_info -------------
    public class app_info
    {
        public app_status status;
        public string app_type;
        public string app_name;
        public Int32 app_id;
        public Int32 partition_count;
    }

    // ---------- node_info -------------
    public class node_info
    {
        public node_status status;
        public dsnrpc_address address;
    }

    // ---------- meta_response_header -------------
    public class meta_response_header
    {
        public dsnerror_code err;
        public dsnrpc_address primary_address;
    }

    // ---------- configuration_update_request -------------
    public class configuration_update_request
    {
        public partition_configuration config;
        public config_type type;
        public dsnrpc_address node;
        public bool is_stateful;
    }

    // ---------- configuration_update_response -------------
    public class configuration_update_response
    {
        public dsnerror_code err;
        public partition_configuration config;
    }

    // ---------- configuration_query_by_node_request -------------
    public class configuration_query_by_node_request
    {
        public dsnrpc_address node;
    }

    // ---------- create_app_options -------------
    public class create_app_options
    {
        public Int32 partition_count;
        public Int32 replica_count;
        public bool success_if_exist;
        public string app_type;
        public bool is_stateful;
    }

    // ---------- configuration_create_app_request -------------
    public class configuration_create_app_request
    {
        public string app_name;
        public create_app_options options;
    }

    // ---------- drop_app_options -------------
    public class drop_app_options
    {
        public bool success_if_not_exist;
    }

    // ---------- configuration_drop_app_request -------------
    public class configuration_drop_app_request
    {
        public string app_name;
        public drop_app_options options;
    }

    // ---------- configuration_list_apps_request -------------
    public class configuration_list_apps_request
    {
        public app_status status;
    }

    // ---------- configuration_list_nodes_request -------------
    public class configuration_list_nodes_request
    {
        public node_status status;
    }

    // ---------- configuration_create_app_response -------------
    public class configuration_create_app_response
    {
        public dsnerror_code err;
        public Int32 appid;
    }

    // ---------- configuration_drop_app_response -------------
    public class configuration_drop_app_response
    {
        public dsnerror_code err;
    }

    // ---------- configuration_list_apps_response -------------
    public class configuration_list_apps_response
    {
        public dsnerror_code err;
        public List< app_info> infos;
    }

    // ---------- configuration_list_nodes_response -------------
    public class configuration_list_nodes_response
    {
        public dsnerror_code err;
        public List< node_info> infos;
    }

    // ---------- configuration_query_by_node_response -------------
    public class configuration_query_by_node_response
    {
        public dsnerror_code err;
        public List< partition_configuration> partitions;
    }

    // ---------- configuration_query_by_index_request -------------
    public class configuration_query_by_index_request
    {
        public string app_name;
        public List< Int32> partition_indices;
    }

    // ---------- configuration_query_by_index_response -------------
    public class configuration_query_by_index_response
    {
        public dsnerror_code err;
        public Int32 app_id;
        public Int32 partition_count;
        public List< partition_configuration> partitions;
    }

    // ---------- query_replica_decree_request -------------
    public class query_replica_decree_request
    {
        public global_partition_id gpid;
        public dsnrpc_address node;
    }

    // ---------- query_replica_decree_response -------------
    public class query_replica_decree_response
    {
        public dsnerror_code err;
        public Int64 last_decree;
    }

} 
