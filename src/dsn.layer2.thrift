
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
}
