#ifndef MISC_FUNCTIONS_H
#define MISC_FUNCTIONS_H

#include <vector>
#include <dsn/service_api_c.h>
#include <dsn/service_api_cpp.h>
#include "dist/replication/meta_server/meta_data.h"
#include "dist/replication/common/fs_manager.h"

typedef std::map<dsn::rpc_address, std::shared_ptr<dsn::replication::fs_manager>> nodes_fs_manager;

inline dsn::replication::fs_manager *get_fs_manager(nodes_fs_manager &nfm,
                                                    const dsn::rpc_address &node)
{
    auto iter = nfm.find(node);
    if (nfm.end() == iter)
        return nullptr;
    return iter->second.get();
}

uint32_t random32(uint32_t min, uint32_t max);

void generate_node_list(/*out*/ std::vector<dsn::rpc_address> &output_list,
                        int min_count,
                        int max_count);

void generate_app(
    /*out*/ std::shared_ptr<dsn::replication::app_state> &app,
    const std::vector<dsn::rpc_address> &node_list);

void generate_node_mapper(
    /*out*/ dsn::replication::node_mapper &output_nodes,
    const dsn::replication::app_mapper &input_apps,
    const std::vector<dsn::rpc_address> &input_node_list);

void generate_app_serving_replica_info(/*out*/ std::shared_ptr<dsn::replication::app_state> &app,
                                       int total_disks);

void generate_node_fs_manager(const dsn::replication::app_mapper &apps,
                              const dsn::replication::node_mapper &nodes,
                              /*out*/ nodes_fs_manager &nfm,
                              int total_disks);

void generate_apps(/*out*/ dsn::replication::app_mapper &apps,
                   const std::vector<dsn::rpc_address> &node_list,
                   int apps_count,
                   int disks_per_node,
                   std::pair<uint32_t, uint32_t> partitions_range,
                   bool generate_serving_info);

// when the test need to track the disk info, please input the fs_manager of all disks,
// the check_apply routine will modify it accordingly.
// if track disk info is not necessary, please input a nullptr.
void migration_check_and_apply(
    /*in-out*/ dsn::replication::app_mapper &apps,
    /*in-out*/ dsn::replication::node_mapper &nodes,
    /*in-out*/ dsn::replication::migration_list &ml,
    /*in-out*/ nodes_fs_manager *manager);

// when the test need to track the disk info, please input the fs_manager of all disks,
// the check_apply routine will modify it accordingly.
// if track disk info is not necessary, please input a nullptr.
void proposal_action_check_and_apply(const dsn::replication::configuration_proposal_action &act,
                                     const dsn::gpid &pid,
                                     dsn::replication::app_mapper &apps,
                                     dsn::replication::node_mapper &nodes,
                                     nodes_fs_manager *manager);

void track_disk_info_check_and_apply(const dsn::replication::configuration_proposal_action &act,
                                     const dsn::gpid &pid,
                                     /*in-out*/ dsn::replication::app_mapper &apps,
                                     /*in-out*/ dsn::replication::node_mapper &nodes,
                                     /*in-out*/ nodes_fs_manager &manager);

void app_mapper_compare(const dsn::replication::app_mapper &mapper1,
                        const dsn::replication::app_mapper &mapper2);

void verbose_apps(const dsn::replication::app_mapper &input_apps);

bool spin_wait_condition(const std::function<bool()> &pred, int seconds);
#endif
