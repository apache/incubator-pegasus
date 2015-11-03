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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include "replication_common.h"
#include <set>

using namespace dsn;
using namespace dsn::service;
using namespace dsn::replication;

namespace dsn {
    namespace replication{
        class replication_checker;
    }
}

typedef std::list<std::pair<::dsn::rpc_address, bool>> node_states;

struct app_state
{
    std::string                          app_type;
    std::string                          app_name;
    int32_t                              app_id;
    int32_t                              partition_count;
    std::vector<partition_configuration> partitions;
};

typedef std::unordered_map<global_partition_id, std::shared_ptr<configuration_update_request> > machine_fail_updates;

class server_state 
{
public:
    server_state(void);
    ~server_state(void);

    // init _app[1] by "[replication.app]" config
    void init_app();

    // get node state std::list<std::pair<::dsn::rpc_address, bool>>
    void get_node_state(/*out*/ node_states& nodes);

    // update node state, maybe:
    //  * add new node
    //  * set node state from live to unlive, and returns configuration_update_request to apply
    //  * set node state from unlive to live, and leaves load balancer to update configuration
    void set_node_state(const node_states& nodes, /*out*/ machine_fail_updates* pris);
    
    // load state from checkpoint file
    void load(const char* chk_point);

    // save state to checkpoint file
    void save(const char* chk_point);

    // partition server & client => meta server

    // query all partition configurations of a replica server
    void query_configuration_by_node(const configuration_query_by_node_request& request, /*out*/ configuration_query_by_node_response& response);

    // query specified partition configurations by app_name and partition indexes
    void query_configuration_by_index(const configuration_query_by_index_request& request, /*out*/ configuration_query_by_index_response& response);

    // query specified partition configuration by gpid
    void query_configuration_by_gpid(global_partition_id id, /*out*/ partition_configuration& config);

    // update partition configuration.
    // first persistent to log file, then apply to memory state
    void update_configuration(const configuration_update_request& request, /*out*/ configuration_update_response& response);

    void unfree_if_possible_on_start();

    // if is freezed
    bool freezed() const { return _freeze.load(); }
    
private:
    // check consistency of memory state, between _nodes, _apps, _node_live_count
    void check_consistency(global_partition_id gpid);

    // do real work of update configuration
    void update_configuration_internal(const configuration_update_request& request, /*out*/ configuration_update_response& response);

private:
    friend class ::dsn::replication::replication_checker;

    struct node_state
    {
        bool                          is_alive;
        ::dsn::rpc_address            address;
        std::set<global_partition_id> primaries;
        std::set<global_partition_id> partitions;
    };

    friend class load_balancer;
    mutable zrwlock_nr                                 _lock;
    std::unordered_map<::dsn::rpc_address, node_state> _nodes;
    std::vector<app_state>                             _apps; // vec_index = app_id - 1

    int                               _node_live_count;
    int                               _node_live_percentage_threshold_for_update;
    std::atomic<bool>                 _freeze;
};

