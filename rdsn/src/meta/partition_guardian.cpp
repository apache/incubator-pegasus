// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "partition_guardian.h"
#include "server_load_balancer.h"
#include <dsn/utils/time_utils.h>

namespace dsn {
namespace replication {
partition_guardian::partition_guardian(meta_service *svc) : _svc(svc)
{
    if (svc != nullptr) {
        _mutation_2pc_min_replica_count = svc->get_options().mutation_2pc_min_replica_count;
        _replica_assign_delay_ms_for_dropouts =
            svc->get_meta_options()._lb_opts.replica_assign_delay_ms_for_dropouts;
        config_context::MAX_REPLICA_COUNT_IN_GRROUP =
            svc->get_meta_options()._lb_opts.max_replicas_in_group;
    } else {
        _mutation_2pc_min_replica_count = 0;
        _replica_assign_delay_ms_for_dropouts = 0;
    }

    _recent_choose_primary_fail_count.init_app_counter(
        "eon.server_load_balancer",
        "recent_choose_primary_fail_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "choose primary fail count in the recent period");
}

pc_status partition_guardian::cure(meta_view view,
                                   const dsn::gpid &gpid,
                                   configuration_proposal_action &action)
{
    if (from_proposals(view, gpid, action))
        return pc_status::ill;

    std::shared_ptr<app_state> &app = (*view.apps)[gpid.get_app_id()];
    const partition_configuration &pc = *get_config(*(view.apps), gpid);
    const proposal_actions &acts = get_config_context(*view.apps, gpid)->lb_actions;

    dassert(app->is_stateful, "");
    dassert(acts.empty(), "");

    pc_status status;
    if (pc.primary.is_invalid())
        status = on_missing_primary(view, gpid);
    else if (static_cast<int>(pc.secondaries.size()) + 1 < pc.max_replica_count)
        status = on_missing_secondary(view, gpid);
    else if (static_cast<int>(pc.secondaries.size()) >= pc.max_replica_count)
        status = on_redundant_secondary(view, gpid);
    else
        status = pc_status::healthy;

    if (!acts.empty()) {
        action = *acts.front();
    }
    return status;
}

void partition_guardian::reconfig(meta_view view, const configuration_update_request &request)
{
    const dsn::gpid &gpid = request.config.pid;
    if (!((*view.apps)[gpid.get_app_id()]->is_stateful)) {
        return;
    }

    config_context *cc = get_config_context(*(view.apps), gpid);
    if (!cc->lb_actions.empty()) {
        const configuration_proposal_action *current = cc->lb_actions.front();
        dassert(current != nullptr && current->type != config_type::CT_INVALID,
                "invalid proposal for gpid(%d.%d)",
                gpid.get_app_id(),
                gpid.get_partition_index());
        // if the valid proposal is from cure
        if (!cc->lb_actions.is_from_balancer()) {
            finish_cure_proposal(view, gpid, *current);
        }
        cc->lb_actions.pop_front();
    }

    // handle the dropped out servers
    if (request.type == config_type::CT_DROP_PARTITION) {
        cc->serving.clear();

        const std::vector<rpc_address> &config_dropped = request.config.last_drops;
        for (const rpc_address &drop_node : config_dropped) {
            cc->record_drop_history(drop_node);
        }
    } else {
        when_update_replicas(request.type, [cc, &request](bool is_adding) {
            if (is_adding) {
                cc->remove_from_dropped(request.node);
                // when some replicas are added to partition_config
                // we should try to adjust the size of drop_list
                cc->check_size();
            } else {
                cc->remove_from_serving(request.node);

                dassert(cc->record_drop_history(request.node),
                        "node(%s) has been in the dropped",
                        request.node.to_string());
            }
        });
    }
}

bool partition_guardian::from_proposals(meta_view &view,
                                        const dsn::gpid &gpid,
                                        configuration_proposal_action &action)
{
    const partition_configuration &pc = *get_config(*(view.apps), gpid);
    config_context &cc = *get_config_context(*(view.apps), gpid);
    bool is_action_valid;

    if (cc.lb_actions.empty()) {
        action.type = config_type::CT_INVALID;
        return false;
    }
    action = *(cc.lb_actions.front());
    char reason[1024];
    if (action.target.is_invalid()) {
        sprintf(reason, "action target is invalid");
        goto invalid_action;
    }
    if (action.node.is_invalid()) {
        sprintf(reason, "action node is invalid");
        goto invalid_action;
    }
    if (!is_node_alive(*(view.nodes), action.target)) {
        sprintf(reason, "action target(%s) is not alive", action.target.to_string());
        goto invalid_action;
    }
    if (!is_node_alive(*(view.nodes), action.node)) {
        sprintf(reason, "action node(%s) is not alive", action.node.to_string());
        goto invalid_action;
    }
    if (cc.lb_actions.is_abnormal_learning_proposal()) {
        sprintf(reason, "learning process abnormal");
        goto invalid_action;
    }

    switch (action.type) {
    case config_type::CT_ASSIGN_PRIMARY:
        is_action_valid = (action.node == action.target && pc.primary.is_invalid() &&
                           !is_secondary(pc, action.node));
        break;
    case config_type::CT_UPGRADE_TO_PRIMARY:
        is_action_valid = (action.node == action.target && pc.primary.is_invalid() &&
                           is_secondary(pc, action.node));
        break;
    case config_type::CT_ADD_SECONDARY:
    case config_type::CT_ADD_SECONDARY_FOR_LB:
        is_action_valid = (is_primary(pc, action.target) && !is_secondary(pc, action.node));
        is_action_valid = (is_action_valid && is_node_alive(*(view.nodes), action.node));
        break;
    case config_type::CT_DOWNGRADE_TO_INACTIVE:
    case config_type::CT_REMOVE:
        is_action_valid = (is_primary(pc, action.target) && is_member(pc, action.node));
        break;
    case config_type::CT_DOWNGRADE_TO_SECONDARY:
        is_action_valid = (action.target == action.node && is_primary(pc, action.target));
        break;
    default:
        is_action_valid = false;
        break;
    }

    if (is_action_valid)
        return true;
    else
        sprintf(reason, "action is invalid");

invalid_action:
    std::stringstream ss;
    ss << action;
    ddebug("proposal action(%s) for gpid(%d.%d) is invalid, clear all proposal actions: %s",
           ss.str().c_str(),
           gpid.get_app_id(),
           gpid.get_partition_index(),
           reason);
    action.type = config_type::CT_INVALID;

    while (!cc.lb_actions.empty()) {
        configuration_proposal_action cpa = *cc.lb_actions.front();
        if (!cc.lb_actions.is_from_balancer()) {
            finish_cure_proposal(view, gpid, cpa);
        }
        cc.lb_actions.pop_front();
    }
    return false;
}

pc_status partition_guardian::on_missing_primary(meta_view &view, const dsn::gpid &gpid)
{
    const partition_configuration &pc = *get_config(*(view.apps), gpid);
    proposal_actions &acts = get_config_context(*view.apps, gpid)->lb_actions;

    char gpid_name[64];
    snprintf(gpid_name, 64, "%d.%d", gpid.get_app_id(), gpid.get_partition_index());

    configuration_proposal_action action;
    pc_status result = pc_status::invalid;

    action.type = config_type::CT_INVALID;
    // try to upgrade a secondary to primary if the primary is missing
    if (pc.secondaries.size() > 0) {
        action.node.set_invalid();

        for (int i = 0; i < pc.secondaries.size(); ++i) {
            node_state *ns = get_node_state(*(view.nodes), pc.secondaries[i], false);
            dassert(ns != nullptr,
                    "invalid secondary address, address = %s",
                    pc.secondaries[i].to_string());
            if (!ns->alive())
                continue;

            // find a node with minimal primaries
            newly_partitions *np = newly_partitions_ext::get_inited(ns);
            if (action.node.is_invalid() ||
                np->less_primaries(*get_newly_partitions(*(view.nodes), action.node),
                                   gpid.get_app_id())) {
                action.node = ns->addr();
            }
        }

        if (action.node.is_invalid()) {
            derror("all nodes for gpid(%s) are dead, waiting for some secondary to come back....",
                   gpid_name);
            result = pc_status::dead;
        } else {
            action.type = config_type::CT_UPGRADE_TO_PRIMARY;
            newly_partitions *np = get_newly_partitions(*(view.nodes), action.node);
            np->newly_add_primary(gpid.get_app_id(), true);

            action.target = action.node;
            result = pc_status::ill;
        }
    }
    // if nothing in the last_drops, it means that this is a newly created partition, so let's
    // just find a node and assign primary for it.
    else if (pc.last_drops.empty()) {
        dsn::rpc_address min_primary_server;
        newly_partitions *min_primary_server_np = nullptr;

        for (auto &pairs : *view.nodes) {
            node_state &ns = pairs.second;
            if (!ns.alive())
                continue;
            newly_partitions *np = newly_partitions_ext::get_inited(&ns);
            // find a node which has minimal primaries
            if (min_primary_server_np == nullptr ||
                np->less_primaries(*min_primary_server_np, gpid.get_app_id())) {
                min_primary_server = ns.addr();
                min_primary_server_np = np;
            }
        }

        if (min_primary_server_np != nullptr) {
            action.node = min_primary_server;
            action.target = action.node;
            action.type = config_type::CT_ASSIGN_PRIMARY;
            min_primary_server_np->newly_add_primary(gpid.get_app_id(), false);
        }

        result = pc_status::ill;
    }
    // well, all replicas in this partition is dead
    else {
        dwarn("%s enters DDD state, we are waiting for all replicas to come back, "
              "and select primary according to informations collected",
              gpid_name);
        // when considering how to handle the DDD state, we must keep in mind that our
        // shared/private-log data only write to OS-cache.
        // so the last removed replica can't act as primary directly.
        std::string reason;
        config_context &cc = *get_config_context(*view.apps, gpid);
        action.node.set_invalid();
        for (int i = 0; i < cc.dropped.size(); ++i) {
            const dropped_replica &dr = cc.dropped[i];
            char time_buf[30];
            ::dsn::utils::time_ms_to_string(dr.time, time_buf);
            ddebug("%s: config_context.dropped[%d]: "
                   "node(%s), time(%" PRIu64 "){%s}, ballot(%" PRId64 "), "
                   "commit_decree(%" PRId64 "), prepare_decree(%" PRId64 ")",
                   gpid_name,
                   i,
                   dr.node.to_string(),
                   dr.time,
                   time_buf,
                   dr.ballot,
                   dr.last_committed_decree,
                   dr.last_prepared_decree);
        }

        for (int i = 0; i < pc.last_drops.size(); ++i) {
            int dropped_index = -1;
            for (int k = 0; k < cc.dropped.size(); k++) {
                if (cc.dropped[k].node == pc.last_drops[i]) {
                    dropped_index = k;
                    break;
                }
            }
            ddebug("%s: config_context.last_drops[%d]: node(%s), dropped_index(%d)",
                   gpid_name,
                   i,
                   pc.last_drops[i].to_string(),
                   dropped_index);
        }

        if (pc.last_drops.size() == 1) {
            dwarn("%s: the only node(%s) is dead, waiting it to come back",
                  gpid_name,
                  pc.last_drops.back().to_string());
            action.node = pc.last_drops.back();
        } else {
            std::vector<dsn::rpc_address> nodes(pc.last_drops.end() - 2, pc.last_drops.end());
            std::vector<dropped_replica> collected_info(2);
            bool ready = true;

            ddebug("%s: last two drops are %s and %s (the latest dropped)",
                   gpid_name,
                   nodes[0].to_string(),
                   nodes[1].to_string());

            for (unsigned int i = 0; i < nodes.size(); ++i) {
                node_state *ns = get_node_state(*view.nodes, nodes[i], false);
                if (ns == nullptr || !ns->alive()) {
                    ready = false;
                    reason = "the last dropped node(" + nodes[i].to_std_string() +
                             ") haven't come back yet";
                    dwarn("%s: don't select primary: %s", gpid_name, reason.c_str());
                } else {
                    std::vector<dropped_replica>::iterator it = cc.find_from_dropped(nodes[i]);
                    if (it == cc.dropped.end() || it->ballot == invalid_ballot) {
                        if (ns->has_collected()) {
                            ddebug("%s: ignore %s's replica info as it doesn't exist on replica "
                                   "server",
                                   gpid_name,
                                   nodes[i].to_string());
                            collected_info[i] = {nodes[i], 0, -1, -1, -1};
                        } else {
                            ready = false;
                            reason = "the last dropped node(" + nodes[i].to_std_string() +
                                     ") is unavailable because ";
                            if (it == cc.dropped.end()) {
                                reason += "the node is not exist in dropped_nodes";
                            } else {
                                reason += "replica info has not been collected from the node";
                            }
                            dwarn("%s: don't select primary: %s", gpid_name, reason.c_str());
                        }
                    } else {
                        collected_info[i] = *it;
                    }
                }
            }

            if (ready && collected_info[0].ballot == -1 && collected_info[1].ballot == -1) {
                ready = false;
                reason = "no replica info collected from the last two drops";
                dwarn("%s: don't select primary: %s", gpid_name, reason.c_str());
            }

            if (ready) {
                dropped_replica &previous_dead = collected_info[0];
                dropped_replica &recent_dead = collected_info[1];

                // 1. larger ballot should have larger committed decree
                // 2. max_prepared_decree should larger than meta's committed decree
                int64_t gap1 = previous_dead.ballot - recent_dead.ballot;
                int64_t gap2 =
                    previous_dead.last_committed_decree - recent_dead.last_committed_decree;
                if (gap1 * gap2 >= 0) {
                    int64_t larger_cd = std::max(previous_dead.last_committed_decree,
                                                 recent_dead.last_committed_decree);
                    int64_t larger_pd = std::max(previous_dead.last_prepared_decree,
                                                 recent_dead.last_prepared_decree);
                    if (larger_pd >= pc.last_committed_decree && larger_pd >= larger_cd) {
                        if (gap1 != 0) {
                            // 1. choose node with larger ballot
                            action.node = gap1 < 0 ? recent_dead.node : previous_dead.node;
                        } else if (gap2 != 0) {
                            // 2. choose node with larger last_committed_decree
                            action.node = gap2 < 0 ? recent_dead.node : previous_dead.node;
                        } else {
                            // 3. choose node with larger last_prepared_decree
                            action.node = previous_dead.last_prepared_decree >
                                                  recent_dead.last_prepared_decree
                                              ? previous_dead.node
                                              : recent_dead.node;
                        }
                        ddebug(
                            "%s: select %s as a new primary", gpid_name, action.node.to_string());
                    } else {
                        char buf[1000];
                        sprintf(buf,
                                "for the last two drops, larger_prepared_decree(%" PRId64 "), "
                                "last committed decree on meta(%" PRId64 "), "
                                "larger_committed_decree(%" PRId64 ")",
                                larger_pd,
                                pc.last_committed_decree,
                                larger_cd);
                        dwarn("%s: don't select primary: %s", gpid_name, reason.c_str());
                    }
                } else {
                    reason = "for the last two drops, the node with larger ballot has smaller last "
                             "committed decree";
                    dwarn("%s: don't select primary: %s", gpid_name, reason.c_str());
                }
            }
        }

        if (!action.node.is_invalid()) {
            action.target = action.node;
            action.type = config_type::CT_ASSIGN_PRIMARY;

            get_newly_partitions(*view.nodes, action.node)
                ->newly_add_primary(gpid.get_app_id(), false);
        } else {
            dwarn("%s: don't select any node for security reason, administrator can select "
                  "a proper one by shell",
                  gpid_name);
            _recent_choose_primary_fail_count->increment();
            ddd_partition_info pinfo;
            pinfo.config = pc;
            for (int i = 0; i < cc.dropped.size(); ++i) {
                const dropped_replica &dr = cc.dropped[i];
                ddd_node_info ninfo;
                ninfo.node = dr.node;
                ninfo.drop_time_ms = dr.time;
                ninfo.ballot = invalid_ballot;
                ninfo.last_committed_decree = invalid_decree;
                ninfo.last_prepared_decree = invalid_decree;
                node_state *ns = get_node_state(*view.nodes, dr.node, false);
                if (ns != nullptr && ns->alive()) {
                    ninfo.is_alive = true;
                    if (ns->has_collected()) {
                        ninfo.is_collected = true;
                        ninfo.ballot = dr.ballot;
                        ninfo.last_committed_decree = dr.last_committed_decree;
                        ninfo.last_prepared_decree = dr.last_prepared_decree;
                    }
                }
                pinfo.dropped.emplace_back(std::move(ninfo));
            }
            pinfo.reason = reason;
            set_ddd_partition(std::move(pinfo));
        }

        result = pc_status::dead;
    }

    if (action.type != config_type::CT_INVALID) {
        acts.assign_cure_proposal(action);
    }
    return result;
}

pc_status partition_guardian::on_missing_secondary(meta_view &view, const dsn::gpid &gpid)
{
    partition_configuration &pc = *get_config(*(view.apps), gpid);
    config_context &cc = *get_config_context(*(view.apps), gpid);

    configuration_proposal_action action;
    bool is_emergency = false;
    if (cc.config_owner->max_replica_count > _mutation_2pc_min_replica_count &&
        replica_count(pc) < _mutation_2pc_min_replica_count) {
        // ATTENTION:
        // when max_replica_count == 2, even if there is only 1 replica alive now, we will still
        // wait for replica_assign_delay_ms_for_dropouts before recover the second replica.
        is_emergency = true;
        ddebug("gpid(%s): is emergency due to too few replicas", gpid.to_string());
    } else if (cc.dropped.empty()) {
        is_emergency = true;
        ddebug("gpid(%s): is emergency due to no dropped candidate", gpid.to_string());
    } else if (has_milliseconds_expired(cc.dropped.back().time +
                                        _replica_assign_delay_ms_for_dropouts)) {
        is_emergency = true;
        char time_buf[30];
        ::dsn::utils::time_ms_to_string(cc.dropped.back().time, time_buf);
        ddebug("gpid(%s): is emergency due to lose secondary for a long time, "
               "last_dropped_node(%s), drop_time(%s), delay_ms(%" PRIu64 ")",
               gpid.to_string(),
               cc.dropped.back().node.to_string(),
               time_buf,
               _replica_assign_delay_ms_for_dropouts);
    } else if (in_black_list(cc.dropped.back().node)) {
        ddebug("gpid(%s) is emergency due to recent dropped(%s) is in black list",
               gpid.to_string(),
               cc.dropped.back().node.to_string());
        is_emergency = true;
    }
    action.node.set_invalid();

    if (is_emergency) {
        std::ostringstream oss;
        for (int i = 0; i < cc.dropped.size(); ++i) {
            if (i != 0)
                oss << ",";
            oss << cc.dropped[i].node.to_string();
        }
        ddebug("gpid(%s): try to choose node in dropped list, dropped_list(%s), "
               "prefered_dropped(%d)",
               gpid.to_string(),
               oss.str().c_str(),
               cc.prefered_dropped);
        if (cc.prefered_dropped < 0 || cc.prefered_dropped >= (int)cc.dropped.size()) {
            ddebug("gpid(%s): prefered_dropped(%d) is invalid according to drop_list(size %d), "
                   "reset it to %d (drop_list.size - 1)",
                   gpid.to_string(),
                   cc.prefered_dropped,
                   (int)cc.dropped.size(),
                   (int)cc.dropped.size() - 1);
            cc.prefered_dropped = (int)cc.dropped.size() - 1;
        }

        while (cc.prefered_dropped >= 0) {
            const dropped_replica &server = cc.dropped[cc.prefered_dropped];
            if (is_node_alive(*view.nodes, server.node)) {
                ddebug("gpid(%s): node(%s) at cc.dropped[%d] is alive now, choose it, "
                       "and forward prefered_dropped from (%d) to (%d)",
                       gpid.to_string(),
                       server.node.to_string(),
                       cc.prefered_dropped,
                       cc.prefered_dropped,
                       cc.prefered_dropped - 1);
                action.node = server.node;
                cc.prefered_dropped--;
                break;
            } else {
                ddebug("gpid(%s): node(%s) at cc.dropped[%d] is not alive now, "
                       "changed prefered_dropped from (%d) to (%d)",
                       gpid.to_string(),
                       server.node.to_string(),
                       cc.prefered_dropped,
                       cc.prefered_dropped,
                       cc.prefered_dropped - 1);
                cc.prefered_dropped--;
            }
        }

        if (action.node.is_invalid() || in_black_list(action.node)) {
            if (!action.node.is_invalid()) {
                ddebug("gpid(%s) refuse to use selected node(%s) as it is in black list",
                       gpid.to_string(),
                       action.node.to_string());
            }
            newly_partitions *min_server_np = nullptr;
            for (auto &pairs : *view.nodes) {
                node_state &ns = pairs.second;
                if (!ns.alive() || is_member(pc, ns.addr()) || in_black_list(ns.addr()))
                    continue;
                newly_partitions *np = newly_partitions_ext::get_inited(&ns);
                if (min_server_np == nullptr ||
                    np->less_partitions(*min_server_np, gpid.get_app_id())) {
                    action.node = ns.addr();
                    min_server_np = np;
                }
            }

            if (!action.node.is_invalid()) {
                ddebug("gpid(%s): can't find valid node in dropped list to add as secondary, "
                       "choose new node(%s) with minimal partitions serving",
                       gpid.to_string(),
                       action.node.to_string());
            } else {
                ddebug("gpid(%s): can't find valid node in dropped list to add as secondary, "
                       "but also we can't find a new node to add as secondary",
                       gpid.to_string());
            }
        }
    } else {
        // if not emergency, only try to recover last dropped server
        const dropped_replica &server = cc.dropped.back();
        if (is_node_alive(*view.nodes, server.node)) {
            dassert(!server.node.is_invalid(),
                    "invalid server address, address = %s",
                    server.node.to_string());
            action.node = server.node;
        }

        if (!action.node.is_invalid()) {
            ddebug("gpid(%s): choose node(%s) as secondary coz it is last_dropped_node and is "
                   "alive now",
                   gpid.to_string(),
                   server.node.to_string());
        } else {
            ddebug("gpid(%s): can't add secondary coz last_dropped_node(%s) is not alive now, "
                   "ignore this as not in emergency",
                   gpid.to_string(),
                   server.node.to_string());
        }
    }

    if (!action.node.is_invalid()) {
        action.type = config_type::CT_ADD_SECONDARY;
        action.target = pc.primary;

        newly_partitions *np = get_newly_partitions(*(view.nodes), action.node);
        dassert(np != nullptr, "");
        np->newly_add_partition(gpid.get_app_id());

        cc.lb_actions.assign_cure_proposal(action);
    }

    return pc_status::ill;
}

pc_status partition_guardian::on_redundant_secondary(meta_view &view, const dsn::gpid &gpid)
{
    const node_mapper &nodes = *(view.nodes);
    const partition_configuration &pc = *get_config(*(view.apps), gpid);
    int target = 0;
    int load = nodes.find(pc.secondaries.front())->second.partition_count();
    for (int i = 0; i != pc.secondaries.size(); ++i) {
        int l = nodes.find(pc.secondaries[i])->second.partition_count();
        if (l > load) {
            load = l;
            target = i;
        }
    }

    configuration_proposal_action action;
    action.type = config_type::CT_REMOVE;
    action.node = pc.secondaries[target];
    action.target = pc.primary;

    // TODO: treat remove as cure proposals too
    get_config_context(*view.apps, gpid)->lb_actions.assign_balancer_proposals({action});
    return pc_status::ill;
}

void partition_guardian::finish_cure_proposal(meta_view &view,
                                              const dsn::gpid &gpid,
                                              const configuration_proposal_action &act)
{
    newly_partitions *np = get_newly_partitions(*(view.nodes), act.node);
    if (np == nullptr) {
        ddebug("can't get the newly_partitions extension structure for node(%s), "
               "the node may be dead and removed",
               act.node.to_string());
    } else {
        if (act.type == config_type::CT_ASSIGN_PRIMARY) {
            np->newly_remove_primary(gpid.get_app_id(), false);
        } else if (act.type == config_type::CT_UPGRADE_TO_PRIMARY) {
            np->newly_remove_primary(gpid.get_app_id(), true);
        } else if (act.type == config_type::CT_UPGRADE_TO_SECONDARY ||
                   act.type == config_type::CT_ADD_SECONDARY) {
            np->newly_remove_partition(gpid.get_app_id());
        }
    }
}

void partition_guardian::register_ctrl_commands()
{
    _ctrl_assign_delay_ms = dsn::command_manager::instance().register_command(
        {"meta.lb.assign_delay_ms"},
        "lb.assign_delay_ms [num | DEFAULT]",
        "control the replica_assign_delay_ms_for_dropouts config",
        [this](const std::vector<std::string> &args) { return ctrl_assign_delay_ms(args); });

    _ctrl_assign_secondary_black_list = dsn::command_manager::instance().register_command(
        {"meta.lb.assign_secondary_black_list"},
        "lb.assign_secondary_black_list [<ip:port,ip:port,ip:port>|clear]",
        "control the assign secondary black list",
        [this](const std::vector<std::string> &args) {
            return ctrl_assign_secondary_black_list(args);
        });
}

void partition_guardian::unregister_ctrl_commands()
{
    UNREGISTER_VALID_HANDLER(_ctrl_assign_delay_ms);
    UNREGISTER_VALID_HANDLER(_ctrl_assign_secondary_black_list);
}

std::string partition_guardian::ctrl_assign_delay_ms(const std::vector<std::string> &args)
{
    std::string result("OK");
    if (args.empty()) {
        result = std::to_string(_replica_assign_delay_ms_for_dropouts);
    } else {
        if (args[0] == "DEFAULT") {
            _replica_assign_delay_ms_for_dropouts =
                _svc->get_meta_options()._lb_opts.replica_assign_delay_ms_for_dropouts;
        } else {
            int32_t v = 0;
            if (!dsn::buf2int32(args[0], v) || v <= 0) {
                result = std::string("ERR: invalid arguments");
            } else {
                _replica_assign_delay_ms_for_dropouts = v;
            }
        }
    }
    return result;
}

std::string
partition_guardian::ctrl_assign_secondary_black_list(const std::vector<std::string> &args)
{
    std::string invalid_arguments("invalid arguments");
    std::stringstream oss;
    if (args.empty()) {
        dsn::zauto_read_lock l(_black_list_lock);
        oss << "get ok: ";
        for (auto iter = _assign_secondary_black_list.begin();
             iter != _assign_secondary_black_list.end();
             ++iter) {
            if (iter != _assign_secondary_black_list.begin())
                oss << ",";
            oss << iter->to_string();
        }
        return oss.str();
    }

    if (args.size() != 1) {
        return invalid_arguments;
    }

    dsn::zauto_write_lock l(_black_list_lock);
    if (args[0] == "clear") {
        _assign_secondary_black_list.clear();
        return "clear ok";
    }

    std::vector<std::string> ip_ports;
    dsn::utils::split_args(args[0].c_str(), ip_ports, ',');
    if (args.size() == 0) {
        return invalid_arguments;
    }

    std::set<dsn::rpc_address> addr_list;
    for (const std::string &s : ip_ports) {
        dsn::rpc_address addr;
        if (!addr.from_string_ipv4(s.c_str())) {
            return invalid_arguments;
        }
        addr_list.insert(addr);
    }
    _assign_secondary_black_list = std::move(addr_list);
    return "set ok";
}

void partition_guardian::get_ddd_partitions(const gpid &pid,
                                            std::vector<ddd_partition_info> &partitions)
{
    zauto_lock l(_ddd_partitions_lock);
    if (pid.get_app_id() == -1) {
        partitions.reserve(_ddd_partitions.size());
        for (const auto &kv : _ddd_partitions) {
            partitions.push_back(kv.second);
        }
    } else if (pid.get_partition_index() == -1) {
        for (const auto &kv : _ddd_partitions) {
            if (kv.first.get_app_id() == pid.get_app_id()) {
                partitions.push_back(kv.second);
            }
        }
    } else {
        auto find = _ddd_partitions.find(pid);
        if (find != _ddd_partitions.end()) {
            partitions.push_back(find->second);
        }
    }
}
} // namespace replication
} // namespace dsn
