
include "dsn.thrift"

namespace cpp dsn

struct partition_configuration
{
    1:dsn.gpid               pid;
    2:i64                    ballot;
    3:i32                    max_replica_count;
    4:dsn.rpc_address        primary;
    5:list<dsn.rpc_address>  secondaries;
    6:list<dsn.rpc_address>  last_drops;
    7:i64                    last_committed_decree;
    8:i32                    partition_flags;
}


struct configuration_query_by_index_request
{
    1:string           app_name;
    2:list<i32>        partition_indices;
}

// for server version > 1.11.2, if err == ERR_FORWARD_TO_OTHERS,
// then the forward address will be put in partitions[0].primary if exist.
struct configuration_query_by_index_response
{
    1:dsn.error_code                err;
    2:i32                           app_id;
    3:i32                           partition_count;
    4:bool                          is_stateful;
    5:list<partition_configuration> partitions;
}

enum app_status
{
    AS_INVALID,
    AS_AVAILABLE,
    AS_CREATING,
    AS_CREATE_FAILED, // depricated
    AS_DROPPING,
    AS_DROP_FAILED, // depricated
    AS_DROPPED,
    AS_RECALLING
}

struct app_info
{
    1:app_status    status = app_status.AS_INVALID;
    2:string        app_type;
    3:string        app_name;
    4:i32           app_id;
    5:i32           partition_count;
    6:map<string, string> envs;
    7:bool          is_stateful;
    8:i32           max_replica_count;
    9:i64           expire_second;

    // new fields added from v1.11.0
    10:i64          create_second;
    11:i64          drop_second;

    // New fields added from v1.12.0
    // Whether this app is duplicating.
    // If true it should prevent its unconfirmed WAL from being compacted.
    12:optional bool duplicating;

    // New fields for partition split
    // If meta server failed during partition split,
    // child partition is not existed on remote stroage, but partition count changed.
    // We use init_partition_count to handle those child partitions while sync_apps_from_remote_stroage
    13:i32          init_partition_count = -1;

    // New fields for bulk load
    // Whether this app is executing bulk load
    14:optional bool    is_bulk_loading = false;
}

// Metadata field of the request in rDSN's thrift protocol (version 1).
// TODO(wutao1): add design doc of the thrift protocol.
struct thrift_request_meta_v1
{
    // The replica's gpid.
    1:optional i32 app_id;
    2:optional i32 partition_index;

    // The timeout of this request that's set on client side.
    3:optional i32 client_timeout;

    // The hash value calculated from the hash key.
    4:optional i64 client_partition_hash;

    // Whether it is a backup request. If true, this request (only if it's a read) can be handled by
    // a secondary replica, which does not guarantee strong consistency.
    5:optional bool is_backup_request;
}
