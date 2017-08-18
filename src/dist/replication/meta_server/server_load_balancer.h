/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     base interface of the server load balancer which defines the scheduling
 *     policy of how to place the partition replica to the nodes
  *
 * Revision history:
 *     2015-12-29, @imzhenyu (Zhenyu Guo), first draft
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/service_api_cpp.h>
#include <dsn/dist/error_code.h>
#include <string>
#include <functional>
#include <memory>
#include <algorithm>
#include "meta_data.h"
#include "meta_service.h"

namespace dsn {
namespace replication {

class server_load_balancer
{
public:
    template <typename T>
    static server_load_balancer *create(meta_service *svc)
    {
        return new T(svc);
    }
    typedef server_load_balancer *(*factory)(meta_service *svc);

public:
    server_load_balancer(meta_service *svc);
    virtual ~server_load_balancer() {}

    virtual void reconfig(meta_view view, const configuration_update_request &request) = 0;
    virtual pc_status
    cure(meta_view view, const dsn::gpid &gpid, configuration_proposal_action &action /*out*/) = 0;

    //
    // Make balancer according to current meta-view
    // params:
    //   list: the returned balance results
    // ret:
    //   if any balancer proposal is generated, return true. Or-else, false
    //
    virtual bool balance(meta_view view, migration_list &list) = 0;

    //
    // When replica infos are collected from replica servers, meta-server
    // will use this to check if a replica on a server is useful
    // params:
    //   node: the owner of the replica info
    //   info: the replica info on node
    // ret:
    //   return true if the replica is accepted as an useful replica. Or-else false.
    //   WARNING: if false is returned, the replica on node may be garbage-collected
    //
    virtual bool
    collect_replica(meta_view view, const dsn::rpc_address &node, const replica_info &info) = 0;

    //
    // Try to construct a replica-group by current replica-infos of a gpid
    // ret:
    //   if construct the replica successfully, return true.
    //   Notice: as long as we can construct something from current infos, we treat it as a success
    //
    virtual bool construct_replica(meta_view view, const gpid &pid, int max_replica_count) = 0;

    void register_proposals(meta_view view,
                            const configuration_balancer_request &req,
                            configuration_balancer_response &resp);
    void apply_balancer(meta_view view, const migration_list &ml);

public:
    typedef std::function<bool(const rpc_address &addr1, const rpc_address &addr2)> node_comparator;
    typedef std::function<bool(const node_state &ns)> node_filter;
    static void sort_node(const node_mapper &nodes,
                          const node_comparator &cmp,
                          const node_filter &filter,
                          std::vector<rpc_address> &result)
    {
        result.clear();
        result.reserve(nodes.size());
        for (auto &iter : nodes) {
            if (filter(iter.second)) {
                result.push_back(iter.first);
            }
        }
        std::sort(result.begin(), result.end(), cmp);
    }

    static void sort_alive_nodes(const node_mapper &nodes,
                                 const node_comparator &cmp,
                                 std::vector<rpc_address> &sorted_node)
    {
        sorted_node.clear();
        sorted_node.reserve(nodes.size());
        for (auto &iter : nodes) {
            if (!iter.first.is_invalid() && iter.second.alive()) {
                sorted_node.push_back(iter.first);
            }
        }
        std::sort(sorted_node.begin(), sorted_node.end(), cmp);
    }

    static node_comparator primary_comparator(const node_mapper &nodes)
    {
        return [&nodes](const rpc_address &r1, const rpc_address &r2) {
            int p1 = nodes.find(r1)->second.primary_count();
            int p2 = nodes.find(r2)->second.primary_count();
            if (p1 != p2)
                return p1 < p2;
            return r1 < r2;
        };
    }

    static node_comparator partition_comparator(const node_mapper &nodes)
    {
        return [&nodes](const rpc_address &r1, const rpc_address &r2) {
            int p1 = nodes.find(r1)->second.partition_count();
            int p2 = nodes.find(r2)->second.partition_count();
            if (p1 != p2)
                return p1 < p2;
            return r1 < r2;
        };
    }

protected:
    meta_service *_svc;
    perf_counter_ _recent_choose_primary_fail_count;
};

class simple_load_balancer : public server_load_balancer
{
public:
    simple_load_balancer(meta_service *svc) : server_load_balancer(svc)
    {
        if (svc != nullptr) {
            mutation_2pc_min_replica_count = svc->get_options().mutation_2pc_min_replica_count;
            replica_assign_delay_ms_for_dropouts =
                svc->get_meta_options()._lb_opts.replica_assign_delay_ms_for_dropouts;
            config_context::MAX_REPLICA_COUNT_IN_GRROUP =
                svc->get_meta_options()._lb_opts.max_replicas_in_group;
        } else {
            mutation_2pc_min_replica_count = 0;
            replica_assign_delay_ms_for_dropouts = 0;
        }
    }
    bool balance(meta_view, migration_list &list) override
    {
        list.clear();
        return false;
    }

    void reconfig(meta_view view, const configuration_update_request &request) override;

    pc_status
    cure(meta_view view, const dsn::gpid &gpid, configuration_proposal_action &action) override;

    bool
    collect_replica(meta_view view, const rpc_address &node, const replica_info &info) override;

    bool construct_replica(meta_view view, const gpid &pid, int max_replica_count) override;

protected:
    // if a proposal is generated by cure, meta will record the POSSIBLE PARTITION COUNT
    // IN FUTURE of a node with module "newly_partitions".
    // the side effect should be eliminated when a proposal is finished, no matter
    // successfully or unsuccessfully
    void finish_cure_proposal(meta_view &view,
                              const dsn::gpid &gpid,
                              const configuration_proposal_action &action);

    bool
    from_proposals(meta_view &view, const dsn::gpid &gpid, configuration_proposal_action &action);

    pc_status on_missing_primary(meta_view &view, const dsn::gpid &gpid);
    pc_status on_missing_secondary(meta_view &view, const dsn::gpid &gpid);
    pc_status on_redundant_secondary(meta_view &view, const dsn::gpid &gpid);

    int32_t mutation_2pc_min_replica_count;
    uint64_t replica_assign_delay_ms_for_dropouts;
};
}
}
