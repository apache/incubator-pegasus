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
    namespace dist{
        class server_load_balancer;
    }
}

typedef std::list<std::pair< ::dsn::rpc_address, bool>> node_states;

struct app_state
{
    app_status::type                     status;
    std::string                          app_type;
    std::string                          app_name;
    bool                                 is_stateful;
    std::string                          package_id;
    int32_t                              app_id;
    int32_t                              partition_count;
    std::vector<partition_configuration> partitions;

    // used only for creating app, to count the number of partitions whose node
    // has been ready on the remote storage
    std::atomic_int                      available_partitions;
    DEFINE_JSON_SERIALIZATION(status, app_type, app_name, is_stateful, package_id, app_id, partition_count, partitions)

    app_state() : status(app_status::AS_DROPPED), app_type(), app_name(), is_stateful(true), package_id(), app_id(0), partitions(), partition_count(0)
    {
        available_partitions.store(0);
    }

    app_state(const app_state& other):
        status(other.status),
        app_type(other.app_type),
        app_name(other.app_name),
        is_stateful(other.is_stateful),
        package_id(other.package_id),
        app_id(other.app_id),
        partition_count(other.partition_count),
        partitions(other.partitions)
    {
        available_partitions.store(other.available_partitions.load());
    }

    app_state(app_state&& other):
        status(other.status),
        app_type(std::move(other.app_type)),
        app_name(std::move(other.app_name)),
        is_stateful(other.is_stateful),
        package_id(std::move(other.package_id)),
        app_id(other.app_id),
        partition_count(other.partition_count),
        partitions(std::move(other.partitions))
    {
        available_partitions.store(other.available_partitions.load());
    }
    app_state& operator = (const app_state &other)
    {
        status = other.status;
        app_type=other.app_type;
        app_name=other.app_name;
        is_stateful = other.is_stateful;
        package_id = other.package_id;
        app_id=other.app_id;
        partition_count=other.partition_count;
        partitions=other.partitions;
        available_partitions.store(other.available_partitions.load());
        return *this;
    }
    app_state& operator = (app_state &&other)
    {
        status = other.status;
        app_type = std::move(other.app_type);
        app_name = std::move(other.app_name);
        is_stateful = other.is_stateful;
        package_id = other.package_id;
        app_id = other.app_id;
        partition_count = other.partition_count;
        partitions = std::move(other.partitions);
        available_partitions.store(other.available_partitions.load());
        return *this;
    }
};

typedef std::unordered_map<global_partition_id, std::shared_ptr<configuration_update_request> > machine_fail_updates;

typedef std::function<void (const std::vector<app_state>& /*new_config*/)> config_change_subscriber;


struct partition_configuration_stateless
{
    ::dsn::replication::partition_configuration& config;

    partition_configuration_stateless(::dsn::replication::partition_configuration& pc)
        : config(pc) {}

    std::vector<dsn::rpc_address>& worker_replicas() { return config.last_drops; }

    std::vector<dsn::rpc_address>& host_replicas() { return config.secondaries; }

};

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

    // configuration operations
    void create_app(configuration_create_app_request& request, /*out*/ configuration_create_app_response& response);
    void drop_app(configuration_drop_app_request& request, /*out*/ configuration_drop_app_response& response);
    void list_apps(configuration_list_apps_request& request, /*out*/ configuration_list_apps_response& response);
    void list_nodes(configuration_list_nodes_request& request, /*out*/ configuration_list_nodes_response& response);

    void unfree_if_possible_on_start();

    // if is freezed
    bool freezed() const { return _freeze.load(); }

    // for test
    void set_config_change_subscriber_for_test(config_change_subscriber subscriber);

    // dump & restore
    error_code dump_from_remote_storage(const char* format, const char* local_path, bool sync_immediately);
    error_code restore_from_local_storage(const char* local_path, bool write_back_to_remote_storage);

public:
    static int32_t _default_max_replica_count;

private:
    // initialize apps in local cache and in remote storage
    error_code initialize_apps();

    // synchronize the state from/to meta state server(i.e., _storage)
    error_code sync_apps_to_remote_storage();
    error_code sync_apps_from_remote_storage();

    // check consistency of memory state, between _nodes, _apps, _node_live_count
    void check_consistency(global_partition_id gpid);

    // do real work of update configuration
    void update_configuration_internal(const configuration_update_request& request, /*out*/ configuration_update_response& response);

    // execute all pending requests according to ballot order
    void exec_pending_requests(global_partition_id gpid);

    void initialize_app(app_state& app, dsn_message_t msg);
    void do_app_drop(app_state& app, dsn_message_t msg);
    void init_app_partition_node(int app_id, int pidx);

    struct storage_work_item;
    void update_configuration_on_remote(std::shared_ptr<storage_work_item>& wi);

    // get the application_id from name, -1 for app doesn't exist
    int32_t get_app_index(const char* app_name) const;

    //path util function in meta_state_service
    std::string get_app_path(const app_state& app) const;
    std::string get_partition_path(const global_partition_id& gpid) const;
    std::string get_partition_path(const app_state& app, int partition_id) const;

    bool set_freeze() const
    {
        if (_nodes.size() < _min_live_node_count_for_unfreeze)
            return true;

        return _node_live_count * 100 < _node_live_percentage_threshold_for_update * static_cast<int>(_nodes.size());
    }
private:
    friend class ::dsn::replication::replication_checker;
    friend class ::dsn::replication::test::test_checker;

    struct node_state
    {
        bool                          is_alive;
        ::dsn::rpc_address            address;
        std::set<global_partition_id> primaries;
        std::set<global_partition_id> partitions;
        DEFINE_JSON_SERIALIZATION(is_alive, address, primaries, partitions)
    };

    friend class dsn::dist::server_load_balancer;
    friend class simple_load_balancer;
    friend class greedy_load_balancer;

    std::string                                         _cluster_root;

    //_cluster_root + "/apps"
    std::string                                         _apps_root;
    mutable zrwlock_nr                                  _lock;
    std::unordered_map< ::dsn::rpc_address, node_state> _nodes;

    /*
     * in the initializing of server_state, we firstly cache all
     * alived nodes detected by fd.
     */
    std::unordered_set<dsn::rpc_address> _cache_alive_nodes;

    std::vector<app_state>               _apps; // vec_index = app_id - 1

    int                               _node_live_count;
    int                               _node_live_percentage_threshold_for_update;
    int                               _min_live_node_count_for_unfreeze;
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
    dsn_handle_t                      _cli_dump_handle;

public:
    void json_state(std::stringstream& out) const;
    static void static_cli_json_state(void* context, int argc, const char** argv, dsn_cli_reply* reply);
    static void static_cli_json_state_cleanup(dsn_cli_reply reply);

    static void static_cli_dump_app_states(void* context, int argc, const char** argv, dsn_cli_reply* reply);
    static void static_cli_dump_app_states_cleanup(dsn_cli_reply reply);
};

