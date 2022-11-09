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

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"
#include "utils/zlocks.h"
#include "utils/command_manager.h"
#include "utils/error_code.h"
#include <string>
#include <functional>
#include <memory>
#include <algorithm>
#include <set>
#include "meta_data.h"
#include "meta_service.h"

namespace dsn {
namespace replication {
/// server load balancer extensions for node_state
/// record the newly assigned but not finished replicas for each node, to make the assigning
/// process more balanced.
class newly_partitions
{
public:
    newly_partitions();
    newly_partitions(node_state *ns);
    node_state *owner;
    int total_primaries;
    int total_partitions;
    std::map<int32_t, int32_t> primaries;
    std::map<int32_t, int32_t> partitions;

    int32_t primary_count(int32_t app_id)
    {
        return owner->primary_count(app_id) + primaries[app_id];
    }
    int32_t partition_count(int32_t app_id)
    {
        return owner->partition_count(app_id) + partitions[app_id];
    }

    int32_t primary_count() { return total_primaries + owner->primary_count(); }
    int32_t partition_count() { return total_partitions + owner->partition_count(); }

    bool less_primaries(newly_partitions &another, int32_t app_id);
    bool less_partitions(newly_partitions &another, int32_t app_id);
    void newly_add_primary(int32_t app_id, bool only_primary);
    void newly_add_partition(int32_t app_id);

    void newly_remove_primary(int32_t app_id, bool only_primary);
    void newly_remove_partition(int32_t app_id);

public:
    static void *s_create(void *related_ns);
    static void s_delete(void *_this);
};
typedef dsn::object_extension_helper<newly_partitions, node_state> newly_partitions_ext;
newly_partitions *get_newly_partitions(node_mapper &mapper, const dsn::rpc_address &addr);

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

    //
    // Make balancer proposals by round according to current meta-view
    // params:
    //   view: current meta-view
    //   list: the returned balance results
    // ret:
    //   if any balancer proposal is generated, return true. Or-else, false
    //
    virtual bool balance(meta_view view, migration_list &list) = 0;

    //
    // Make full balancer proposals according to current meta-view
    // params:
    //   view: current meta-view
    //   list: the returned balance results
    // ret:
    //   if any balancer proposal is generated, return true. Or-else, false
    //
    virtual bool check(meta_view view, migration_list &list) = 0;

    //
    // Report balancer proposals
    // params:
    //   list: balancer proposals
    //   balance_checker: report the count of balance operation to be done if true, otherwise report
    //   both the operation count and action details done by balancer
    //
    virtual void report(const migration_list &list, bool balance_checker) = 0;

    //
    // Calculate cluster balance score
    // params:
    //   view: current meta-view
    //   primary_stddev: output, stddev of primary count on each node
    //   total_stddev: output, stddev of total replica count on each node
    //
    virtual void
    score(meta_view view, double &primary_stddev /*out*/, double &total_stddev /*out*/) = 0;

    void register_proposals(meta_view view,
                            const configuration_balancer_request &req,
                            configuration_balancer_response &resp);
    void apply_balancer(meta_view view, const migration_list &ml);

    //
    // Try to register some cli-commands
    //
    // ATTENTION: because this function will register the cli-commands to singleton-container,
    // so
    // you must unregister the commands that you have already registered or release the instance
    // of
    // server_load_balancer before you call this function again
    //
    virtual void register_ctrl_commands() {}

    //
    // Try to unregister cli-commands
    //
    virtual void unregister_ctrl_commands() {}

    //
    // Get balancer proposal counts
    // params:
    //   args: proposal type
    // ret: balancer proposal counts in string
    //
    virtual std::string get_balance_operation_count(const std::vector<std::string> &args) = 0;

public:
    typedef std::function<bool(const rpc_address &addr1, const rpc_address &addr2)> node_comparator;
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
};
} // namespace replication
} // namespace dsn
