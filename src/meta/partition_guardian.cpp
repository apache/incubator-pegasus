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
namespace dsn {
namespace replication {
partition_guardian::partition_guardian(meta_service *svc) : _svc(svc) {}

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
    // TBD(zlw)
    return pc_status::invalid;
}

pc_status partition_guardian::on_missing_secondary(meta_view &view, const dsn::gpid &gpid)
{
    // TBD(zlw)
    return pc_status::invalid;
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
    // TBD(zlw):
}
} // namespace replication
} // namespace dsn
