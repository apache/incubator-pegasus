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

    // get primary meta server
    bool get_meta_server_primary(/*out*/ ::dsn::rpc_address& node);

    // add meta node, if is the first one, set as primary
    void add_meta_node(const ::dsn::rpc_address& node);

    // remove meta node, if is the primary, randomly choose another one as new primary
    void remove_meta_node(const ::dsn::rpc_address& node);

    // randomly choose another one as new primary
    void switch_meta_primary();

    // load state from checkpoint file
    void load(const char* chk_point);

    // save state to checkpoint file
    void save(const char* chk_point);

    // partition server & client => meta server

    // query all partition configurations of a replica server
    void query_configuration_by_node(configuration_query_by_node_request& request, /*out*/ configuration_query_by_node_response& response);

    // query specified partition configurations by app_name and partition indexes
    void query_configuration_by_index(configuration_query_by_index_request& request, /*out*/ configuration_query_by_index_response& response);

    // query specified partition configuration by gpid
    void query_configuration_by_gpid(global_partition_id id, /*out*/ partition_configuration& config);

    // update partition configuration.
    // first persistent to log file, then apply to memory state
    void update_configuration(configuration_update_request& request, /*out*/ configuration_update_response& response);

    void unfree_if_possible_on_start();

    // if is freezed
    bool freezed() const { return _freeze.load(); }
    
private:
    // check consistency of memory state, between _nodes, _apps, _node_live_count
    void check_consistency(global_partition_id gpid);

    // do real work of update configuration
    void update_configuration_internal(configuration_update_request& request, /*out*/ configuration_update_response& response);

private:
    friend class ::dsn::replication::replication_checker;

    struct node_state
    {
        bool                          is_alive;
        ::dsn::rpc_address            address;
        std::set<global_partition_id> primaries;
        std::set<global_partition_id> partitions;
    };

    mutable zrwlock_nr                                 _lock;
    std::unordered_map<::dsn::rpc_address, node_state> _nodes;
    std::vector<app_state>                             _apps; // vec_index = app_id - 1

    int                               _node_live_count;
    int                               _node_live_percentage_threshold_for_update;
    std::atomic<bool>                 _freeze;

    mutable zrwlock_nr                _meta_lock;
    std::vector<::dsn::rpc_address>   _meta_servers;
    int                               _leader_index;

    friend class load_balancer;
};

