#ifndef MISC_FUNCTIONS_H
#define MISC_FUNCTIONS_H

#include <vector>
#include <dsn/service_api_c.h>
#include <dsn/service_api_cpp.h>
#include <dsn/dist/replication/replication.types.h>
#include "meta_data.h"

uint32_t random32(uint32_t min, uint32_t max);

void generate_node_list(/*out*/std::vector<dsn::rpc_address> &output_list, int min_count, int max_count);

void generate_app(
    /*out*/std::shared_ptr<dsn::replication::app_state>& app,
    const std::vector<dsn::rpc_address>& node_list);

void generate_node_mapper(
    /*out*/dsn::replication::node_mapper& output_nodes,
    const dsn::replication::app_mapper& input_apps,
    const std::vector<dsn::rpc_address>& input_node_list
);

void migration_check_and_apply(
    /*in-out*/dsn::replication::app_mapper& apps,
    /*in-out*/dsn::replication::node_mapper& nodes,
    /*in-out*/dsn::replication::migration_list& ml
);

void proposal_action_check_and_apply(
    const dsn::replication::configuration_proposal_action& act,
    const dsn::gpid& pid,
    dsn::replication::app_mapper& apps,
    dsn::replication::node_mapper& nodes);

void app_mapper_compare(const dsn::replication::app_mapper& mapper1, const dsn::replication::app_mapper& mapper2);

void verbose_apps(const dsn::replication::app_mapper& input_apps);

bool spin_wait_condition(const std::function<bool ()>& pred, int seconds);
#endif
