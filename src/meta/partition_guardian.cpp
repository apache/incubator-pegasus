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
    // TBD(zlw)
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
} // namespace replication
} // namespace dsn
