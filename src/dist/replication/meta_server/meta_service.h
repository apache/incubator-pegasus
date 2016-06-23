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
 *     meta server service for EON (rDSN layer 2)
 *
 * Revision history:
 *     2015-03-09, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

#include <memory>

#include <dsn/cpp/serverlet.h>
#include <dsn/dist/meta_state_service.h>
#include "replication_common.h"
#include "meta_options.h"

class meta_service_test_app;
namespace dsn { namespace replication {

class server_state;
class meta_server_failure_detector;
class server_load_balancer;
class replication_checker;
namespace test { class test_checker; }

class meta_service : public serverlet<meta_service>
{
public:
    meta_service();
    virtual ~meta_service();

    error_code start();

    const replication_options& get_options() const { return _opts; }
    const meta_options& get_meta_options() const { return _meta_opts; }
    dist::meta_state_service* get_remote_storage() { return _storage.get(); }
    server_load_balancer* get_balancer() { return _balancer.get(); }
    int64_t get_control_flags() const { return _meta_ctrl_flags; }
    bool is_service_freezed() const { return (_meta_ctrl_flags&meta_ctrl_flags::ctrl_meta_freeze) || check_freeze(); }

    virtual void reply_message(dsn_message_t, dsn_message_t response) { dsn_rpc_reply(response); }
    virtual void send_message(const rpc_address& target, dsn_message_t request) { dsn_rpc_call_one_way(target.c_addr(), request); }

    // these two callbacks are running in fd's thread_pool, and in fd's lock
    void set_node_state(const std::vector<rpc_address>& nodes_list, bool is_alive);
    void get_node_state(/*out*/std::set<rpc_address>&, bool is_alive);

    void prepare_service_starting();
    void service_starting();
    void balancer_run();

private:
    void register_rpc_handlers();

    // partition server & client => meta server
    // query partition configuration
    void on_query_configuration_by_node(dsn_message_t req);
    void on_query_configuration_by_index(dsn_message_t req);

    // update configuration
    void on_propose_balancer(dsn_message_t req);
    void on_update_configuration(dsn_message_t req);

    // table operations
    void on_create_app(dsn_message_t req);
    void on_drop_app(dsn_message_t req);
    void on_list_apps(dsn_message_t req);
    void on_list_nodes(dsn_message_t req);

    // cluster info
    void on_query_cluster_info(dsn_message_t req);

    // meta control
    void on_control_meta(dsn_message_t req);

    // common routines
    int check_primary(dsn_message_t req);

    error_code remote_storage_initialize();
    bool check_freeze() const
    {
        int total = _alive_set.size() + _dead_set.size();
        return (_alive_set.size()<_meta_opts.min_live_node_count_for_unfreeze) ||
                (_alive_set.size()*100<=_node_live_percentage_threshold_for_update*total);
    }
private:
    friend class replication_checker;
    friend class test::test_checker;
    friend class ::meta_service_test_app;

    replication_options _opts;
    meta_options _meta_opts;

    std::shared_ptr<server_state> _state;
    std::shared_ptr<meta_server_failure_detector> _failure_detector;
    std::shared_ptr<dist::meta_state_service> _storage;
    std::shared_ptr<server_load_balancer> _balancer;

    mutable zrwlock_nr _meta_lock;
    std::set<rpc_address> _alive_set;
    std::set<rpc_address> _dead_set;

    int  _node_live_percentage_threshold_for_update;
    volatile bool _started;
    volatile int64_t _meta_ctrl_flags;

    std::string _cluster_root;
};

}}
