include "base.thrift"

namespace go admin 

struct create_app_options
{
    1:i32              partition_count;
    2:i32              replica_count;
    3:bool             success_if_exist;
    4:string           app_type;
    5:bool             is_stateful;
    6:map<string, string>  envs;
}

struct create_app_request
{
    1:string                   app_name;
    2:create_app_options       options;
}

struct create_app_response
{
    1:base.error_code err;
    2:i32 appid;
}

struct drop_app_options
{
    1:bool             success_if_not_exist;
    2:optional i64     reserve_seconds;
}

struct drop_app_request
{
    1:string                   app_name;
    2:drop_app_options         options;
}

struct drop_app_response
{
    1:base.error_code err;
}

enum app_status
{
    AS_INVALID,
    AS_AVAILABLE,
    AS_CREATING,
    AS_CREATE_FAILED, // deprecated
    AS_DROPPING,
    AS_DROP_FAILED, // deprecated
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

struct list_apps_request
{
    1:app_status status = app_status.AS_INVALID;
}

struct list_apps_response
{
    1:base.error_code err
    2:list<app_info> infos;
}

service admin_client 
{
    create_app_response create_app(1:create_app_request req);
    drop_app_response drop_app(1:drop_app_request req);
    list_apps_response list_apps(1:list_apps_request req);
}
