include "base.thrift"

namespace go radmin

struct disk_info
{
    1:string tag;
    2:string full_dir;
    3:i64 disk_capacity_mb;
    4:i64 disk_available_mb;
    // app_id=>set<gpid>
    5:map<i32,set<base.gpid>> holding_primary_replicas;
    6:map<i32,set<base.gpid>> holding_secondary_replicas;
}

// This request is sent from client to replica_server.
struct query_disk_info_request
{
    1:base.rpc_address node;
    2:string          app_name;
}

// This response is from replica_server to client.
struct query_disk_info_response
{
    // app not existed will return "ERR_OBJECT_NOT_FOUND", otherwise "ERR_OK"
    1:base.error_code err;
    2:i64 total_capacity_mb;
    3:i64 total_available_mb;
    4:list<disk_info> disk_infos;
}

// This request is sent from client to replica_server.
struct replica_disk_migrate_request
{
    1:base.gpid pid
    // disk tag, for example `ssd1`. `origin_disk` and `target_disk` must be specified in the config of [replication] data_dirs.
    2:string origin_disk;
    3:string target_disk;
}

// This response is from replica_server to client.
struct replica_disk_migrate_response
{
   // Possible error:
   // -ERR_OK: start do replica disk migrate
   // -ERR_BUSY: current replica migration is running
   // -ERR_INVALID_STATE: current replica partition status isn't secondary
   // -ERR_INVALID_PARAMETERS: origin disk is equal with target disk
   // -ERR_OBJECT_NOT_FOUND: replica not found, origin or target disk isn't existed, origin disk doesn't exist current replica
   // -ERR_PATH_ALREADY_EXIST: target disk has existed current replica
   1:base.error_code err;
   2:optional string hint;
}

// A client to ReplicaServer's administration API.
service replica_client 
{
    query_disk_info_response query_disk_info(1:query_disk_info_request req);
    
    replica_disk_migrate_response disk_migrate(1:replica_disk_migrate_request req);
}
