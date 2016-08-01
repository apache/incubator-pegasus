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
 *     the meta server's server_state, definition file
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     2016-04-25, Weijie Sun(sunweijie at xiaomi.com), refactor
 */

#pragma once

# include <unordered_map>
# include <boost/lexical_cast.hpp>

# include <dsn/dist/replication/replication_other_types.h>

# include "replication_common.h"
# include "meta_data.h"

class meta_service_test_app;

namespace dsn { namespace replication {

class replication_checker;
namespace test { class test_checker; }

typedef std::function<void (const app_mapper& /*new_config*/)> config_change_subscriber;
typedef std::function<void (const migration_list& )> replica_migration_subscriber;

class meta_service;

class server_state
{
public:
    static const int s_state_write_hash = 0;
public:
    server_state();
    ~server_state();

    void initialize(meta_service* meta_svc, const std::string& apps_root);
    error_code initialize_data_structure();
    void register_cli_commands();

    const meta_view get_meta_view() { return {&_all_apps, &_nodes}; }

    // query state
    void query_configuration_by_node(const configuration_query_by_node_request& request, /*out*/ configuration_query_by_node_response& response);
    void query_configuration_by_index(const configuration_query_by_index_request& request, /*out*/ configuration_query_by_index_response& response);
    bool query_configuration_by_gpid(const dsn::gpid id, /*out*/ partition_configuration& config);

    // table options
    void create_app(dsn_message_t msg);
    void drop_app(dsn_message_t msg);
    void list_apps(const configuration_list_apps_request& request, configuration_list_apps_response& response);

    // update configuration
    void on_config_sync(dsn_message_t msg);
    void on_update_configuration(std::shared_ptr<configuration_update_request>& request, dsn_message_t msg);

    // dump & restore
    error_code dump_from_remote_storage(const char* local_path, bool sync_immediately);
    error_code restore_from_local_storage(const char* local_path);

    void on_change_node_state(rpc_address node, bool is_alive);
    void on_propose_balancer(const configuration_balancer_request& request, configuration_balancer_response& response);
    //return true if no need to do any actions
    bool check_all_partitions();
    void clear_proposals();

    // for test
    void set_config_change_subscriber_for_test(config_change_subscriber subscriber);
    void set_replica_migration_subscriber_for_test(replica_migration_subscriber subscriber);
private:
    //-1 means waiting forever
    bool spin_wait_creating(int timeout_seconds = -1);
    bool is_server_state_stable(int healthy_partitions);

    error_code dump_app_states(const char* local_path, const std::function<app_state* ()>& iterator);
    error_code sync_apps_from_remote_storage();
    error_code sync_apps_to_remote_storage();
    error_code initialize_default_apps();
    void initialize_node_state();

    void check_consistency(const dsn::gpid& gpid);

    void do_app_create(std::shared_ptr<app_state>& app, dsn_message_t msg);
    void do_app_drop(std::shared_ptr<app_state> &app, dsn_message_t msg);
    void init_app_partition_node(std::shared_ptr<app_state> &app, int pidx);

    task_ptr update_configuration_on_remote(std::shared_ptr<configuration_update_request>& config_request);
    void on_update_configuration_on_remote_reply(error_code ec, std::shared_ptr<configuration_update_request>& request);
    void update_configuration_locally(app_state& app, std::shared_ptr<configuration_update_request>& config_request);
    void apply_migration_actions(migration_list& ml);
    void request_check(const partition_configuration& old, const configuration_update_request& request);
    void downgrade_primary_to_inactive(std::shared_ptr<app_state>& app, int pidx);
    void downgrade_secondary_to_inactive(std::shared_ptr<app_state>& app, int pidx, const rpc_address& node);
    void downgrade_stateless_nodes(std::shared_ptr<app_state>& app, int pidx, const rpc_address& address);
    void on_partition_node_dead(std::shared_ptr<app_state>& app, int pidx, const dsn::rpc_address& address);
    void send_proposal(rpc_address target, const configuration_update_request& proposal);
    void send_proposal(const configuration_proposal_action& action, const partition_configuration& pc, const app_state &app);

    //util function
    int32_t next_app_id() const
    {
        if (_all_apps.empty())
            return 1;
        //return the max_id + 1
        return ((--_all_apps.end())->first) + 1;
    }
    std::string get_app_path(const app_state& app) const
    {
        return _apps_root + "/" + boost::lexical_cast<std::string>(app.app_id);
    }
    std::string get_partition_path(const dsn::gpid& gpid) const
    {
        std::stringstream oss;
        oss << _apps_root << "/" << gpid.get_app_id() << "/" << gpid.get_partition_index();
        return oss.str();
    }
    std::string get_partition_path(const app_state& app, int partition_id) const
    {
        std::stringstream oss;
        oss << _apps_root << "/" << app.app_id << "/" << partition_id;
        return oss.str();
    }
    std::shared_ptr<app_state> get_app(const std::string& name)
    {
        auto iter = _exist_apps.find(name);
        if (iter == _exist_apps.end())
            return nullptr;
        return iter->second;
    }
    std::shared_ptr<app_state> get_app(int32_t app_id)
    {
        auto iter = _all_apps.find(app_id);
        if (iter == _all_apps.end())
            return nullptr;
        return iter->second;
    }
    void inc_creating_app_available_partitions(std::shared_ptr<app_state>& app)
    {
        int ans = ++app->helpers->available_partitions;
        if (ans == app->partition_count)
        {
            zauto_write_lock l(_lock);
            app->status = app_status::AS_AVAILABLE;
            --_creating_apps_count;
        }
    }
private:
    friend class replication_checker;
    friend class test::test_checker;
    friend class ::meta_service_test_app;

    meta_service*                                       _meta_svc;
    std::string                                         _apps_root;

    mutable zrwlock_nr                                  _lock;
    node_mapper                                         _nodes;

    //available apps, dropping apps, creating apps: name -> app_state
    std::map< std::string, std::shared_ptr<app_state> > _exist_apps;
    //_exist_apps + dropped apps: app_id -> app_state
    app_mapper                                          _all_apps;

    std::atomic_int                                     _creating_apps_count;
    std::atomic_int                                     _dropping_apps_count;

    //for load balancer
    migration_list                                      _temporary_list;

    // for test
    config_change_subscriber                            _config_change_subscriber;
    replica_migration_subscriber                        _replica_migration_subscriber;

    dsn_handle_t                                        _cli_json_state_handle;
    dsn_handle_t                                        _cli_dump_handle;
public:
    void json_state(std::stringstream& out) const;
    static void static_cli_json_state(void* context, int argc, const char** argv, dsn_cli_reply* reply);
    static void static_cli_json_state_cleanup(dsn_cli_reply reply);

    static void static_cli_dump_app_states(void* context, int argc, const char** argv, dsn_cli_reply* reply);
    static void static_cli_dump_app_states_cleanup(dsn_cli_reply reply);
};

}}
