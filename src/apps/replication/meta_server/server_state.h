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

# include <dsn/dist/replication/replication_other_types.h>
# include "replication_common.h"
# include <dsn/dist/meta_state_service.h>
# include <set>
# include <unordered_set>
# include <list>

using namespace dsn;
using namespace dsn::service;
using namespace dsn::replication;

namespace dsn {
    namespace replication{
        class replication_checker;
        namespace test {
            class test_checker;
        }
    }
}

typedef std::list<std::pair< ::dsn::rpc_address, bool>> node_states;

enum app_status
{
    available,
    creating,
    creating_failed
};

struct app_state
{
    app_status                           status;
    std::string                          app_type;
    std::string                          app_name;
    int32_t                              app_id;
    int32_t                              partition_count;
    std::vector<partition_configuration> partitions;
    DEFINE_JSON_SERIALIZATION(app_type, app_name, app_id, partition_count, partitions);
};

typedef std::unordered_map<global_partition_id, std::shared_ptr<configuration_update_request> > machine_fail_updates;

typedef std::function<void (const std::vector<app_state>& /*new_config*/)> config_change_subscriber;

class server_state :
    public ::dsn::serverlet<server_state>
{
public:
    server_state();
    virtual ~server_state();

    // initialize server state
    error_code initialize();

    // when the server becomes the leader
    error_code on_become_leader();

    // get node state std::list<std::pair< ::dsn::rpc_address, bool>>
    void get_node_state(/*out*/ node_states& nodes);

    // update node state, maybe:
    //  * add new node
    //  * set node state from live to unlive, and returns configuration_update_request to apply
    //  * set node state from unlive to live, and leaves load balancer to update configuration
    void set_node_state(const node_states& nodes, /*out*/ machine_fail_updates* pris);

    // add alive node to cache when initializing
    void add_alive_node_to_cache(const rpc_address& node) { _cache_alive_nodes.insert(node); }

    // remove dead node from cache when initializing
    void remove_dead_node_from_cache(const rpc_address& node) { _cache_alive_nodes.erase(node); }

    // apply the cache
    void apply_cache_nodes();

    // partition server & client => meta server

    // query all partition configurations of a replica server
    void query_configuration_by_node(const configuration_query_by_node_request& request, /*out*/ configuration_query_by_node_response& response);

    // query specified partition configurations by app_name and partition indexes
    void query_configuration_by_index(const configuration_query_by_index_request& request, /*out*/ configuration_query_by_index_response& response);

    // query specified partition configuration by gpid
    void query_configuration_by_gpid(global_partition_id id, /*out*/ partition_configuration& config);

    // update partition configuration.
    // first persistent to log file, then apply to memory state
    //void update_configuration(const configuration_update_request& request, /*out*/ configuration_update_response& response);

    // TODO: callback should pass in error_code
    void update_configuration(
        std::shared_ptr<configuration_update_request>& req,
        dsn_message_t request_msg,
        std::function<void()> callback
        );

    // create table
    void create_table(dsn_message_t msg);
    void unfree_if_possible_on_start();

    // if is freezed
    bool freezed() const { return _freeze.load(); }

    // for test
    void set_config_change_subscriber_for_test(config_change_subscriber subscriber);

private:
    // initialize apps in local cache and in remote storage
    error_code initialize_apps();

    // synchronize the latest state from meta state server (i.e., _storage)
    error_code sync_apps_from_remote_storage();

    // check consistency of memory state, between _nodes, _apps, _node_live_count
    void check_consistency(global_partition_id gpid);

    // do real work of update configuration
    void update_configuration_internal(const configuration_update_request& request, /*out*/ configuration_update_response& response);

    // execute all pending requests according to ballot order
    void exec_pending_requests(global_partition_id gpid);

    // compute drop out collection
    void maintain_drops(/*inout*/ std::vector<rpc_address>& drops, const rpc_address& node, bool is_add);

    // check equality of two partition configurations, not take last_drops into account
    bool partition_configuration_equal(const partition_configuration& pc1, const partition_configuration& pc2);

    // get the application_id from name, -1 for app doesn't exist
    int32_t app_id(const char* app_name) const;

    // join path
    static std::string join_path(const std::string& input1, const std::string& input2);
private:
    friend class ::dsn::replication::replication_checker;
    friend class ::dsn::replication::test::test_checker;

    struct node_state
    {
        bool                          is_alive;
        ::dsn::rpc_address            address;
        std::set<global_partition_id> primaries;
        std::set<global_partition_id> partitions;
        DEFINE_JSON_SERIALIZATION(is_alive, address, primaries, partitions);
    };

    friend class simple_stateful_load_balancer;
    std::string                                         _cluster_root;
    mutable zrwlock_nr                                  _lock;
    std::unordered_map< ::dsn::rpc_address, node_state> _nodes;

    /*
     * in the initializing of server_state, we firstly cache all
     * alived nodes detected by fd.
     */
    std::unordered_set<dsn::rpc_address> _cache_alive_nodes;

    std::vector<app_state>                              _apps; // vec_index = app_id - 1

    int                               _node_live_count;
    int                               _node_live_percentage_threshold_for_update;
    std::atomic<bool>                 _freeze;

    ::dsn::dist::meta_state_service *_storage;
    struct storage_work_item
    {
        int64_t ballot;
        std::shared_ptr<configuration_update_request> req;
        dsn_message_t msg;
        std::function<void()> callback;
    };

    mutable zlock                     _pending_requests_lock;
    // because ballots of different gpid may conflict, we separate items by gpid
    // gpid => <ballot, item>
    std::map<global_partition_id, std::map<int64_t, storage_work_item> > _pending_requests;

    // for test
    config_change_subscriber          _config_change_subscriber;

    dsn_handle_t                      _cli_json_state_handle;

public:
    void json_state(std::stringstream& out) const;
    static void static_cli_json_state(void* context, int argc, const char** argv, dsn_cli_reply* reply);
    static void static_cli_json_state_cleanup(dsn_cli_reply reply);
};

