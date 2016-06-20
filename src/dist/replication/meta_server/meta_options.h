#pragma once

#include <string>
#include <dsn/dist/replication.h>
#include <dsn/dist/replication/replication.types.h>

namespace dsn { namespace replication {

class meta_options
{
public:
    std::string cluster_root;
    std::string meta_state_service_type;
    std::string distributed_lock_service_type;
    std::string server_load_balancer_type;

    std::vector<std::string> meta_state_service_args;
    std::vector<std::string> distributed_lock_service_args;

    uint64_t replica_assign_delay_ms_for_dropouts;
    uint64_t node_live_percentage_threshold_for_update;
    uint64_t min_live_node_count_for_unfreeze;
public:
    void initialize();

public:
    static std::string concat_path_unix_style(const std::string& prefix, const std::string& postfix);
};

}}
