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

//
// the replica_stub is the *singleton* entry to
// access all replica managed in the same process
//   replica_stub(singleton) --> replica --> replication_app
//

# include "replication_common.h"
# include <dsn/cpp/perf_counter_.h>

namespace dsn { namespace replication {

class mutation_log;
class replication_failure_detector;
class replication_checker;
namespace test {
    class test_checker;
}

typedef std::unordered_map<global_partition_id, replica_ptr> replicas;
typedef std::function<void (::dsn::rpc_address /*from*/,
                            const replica_configuration& /*new_config*/,
                            bool /*is_closing*/)> replica_state_subscriber;

class replica_stub : public serverlet<replica_stub>, public ref_counter
{
public:
    static bool s_not_exit_on_log_failure; // for test

public:
    replica_stub(replica_state_subscriber subscriber = nullptr, bool is_long_subscriber = true);
    ~replica_stub(void);

    //
    // initialization
    //
    void initialize(const replication_options& opts, bool clear = false);
    void initialize(bool clear = false);
    void set_options(const replication_options& opts) { _options = opts; }
    void open_service();
    void close();

    //
    //    requests from clients
    //
    void on_client_write(dsn_message_t request);
    void on_client_read(dsn_message_t request);

    //
    //    messages from meta server
    //
    void on_config_proposal(const configuration_update_request& proposal);
    void on_query_decree(const query_replica_decree_request& req, /*out*/ query_replica_decree_response& resp);
        
    //
    //    messages from peers (primary or secondary)
    //        - prepare
    //        - commit
    //        - learn
    //
    void on_prepare(dsn_message_t request);    
    void on_learn(dsn_message_t msg);
    void on_learn_completion_notification(const group_check_response& report);
    void on_add_learner(const group_check_request& request);
    void on_remove(const replica_configuration& request);
    void on_group_check(const group_check_request& request, /*out*/ group_check_response& response);
    void on_copy_checkpoint(const replica_configuration& request, /*out*/ learn_response& response);

    //
    //    local messages
    //
    void on_meta_server_connected();
    void on_meta_server_disconnected();
    void on_gc();

    //
    //  routines published for test
    // 
    void init_gc_for_test();
    void set_meta_server_disconnected_for_test() { on_meta_server_disconnected(); }
    void set_meta_server_connected_for_test(const configuration_query_by_node_response& config);
    void set_replica_state_subscriber_for_test(replica_state_subscriber subscriber, bool is_long_subscriber);

    //
    // common routines for inquiry
    //
    const std::string& dir() const { return _dir; }
    replica_ptr get_replica(global_partition_id gpid, bool new_when_possible = false, const char* app_type = nullptr);
    replica_ptr get_replica(int32_t app_id, int32_t partition_index);
    replication_options& options() { return _options; }
    bool is_connected() const { return NS_Connected == _state; }

    void json_state(std::stringstream& out) const;

    static void static_replica_stub_json_state(void* context, int argc, const char** argv, dsn_cli_reply* reply);
    static void static_replica_stub_json_state_freer(dsn_cli_reply reply);

private:    
    enum replica_node_state
    {
        NS_Disconnected,
        NS_Connecting,
        NS_Connected
    };

    void query_configuration_by_node();
    void on_meta_server_disconnected_scatter(replica_stub_ptr this_, global_partition_id gpid);
    void on_node_query_reply(error_code err, dsn_message_t request, dsn_message_t response);
    void on_node_query_reply_scatter(replica_stub_ptr this_, const partition_configuration& config);
    void on_node_query_reply_scatter2(replica_stub_ptr this_, global_partition_id gpid);
    void remove_replica_on_meta_server(const partition_configuration& config);
    ::dsn::task_ptr begin_open_replica(const std::string& app_type, global_partition_id gpid, std::shared_ptr<group_check_request> req = nullptr);
    void    open_replica(const std::string app_type, global_partition_id gpid, std::shared_ptr<group_check_request> req);
    ::dsn::task_ptr begin_close_replica(replica_ptr r);
    void close_replica(replica_ptr r);
    void add_replica(replica_ptr r);
    bool remove_replica(replica_ptr r);
    void notify_replica_state_update(const replica_configuration& config, bool is_closing);
    void handle_log_failure(error_code err);

    void install_perf_counters();

private:
    friend class ::dsn::replication::replication_checker;    
    friend class ::dsn::replication::test::test_checker;
    friend class ::dsn::replication::replica;
    typedef std::unordered_map<global_partition_id, ::dsn::task_ptr> opening_replicas;
    typedef std::unordered_map<global_partition_id, std::pair< ::dsn::task_ptr, replica_ptr>> closing_replicas; // <close, replica>

    mutable zlock               _replicas_lock;
    replicas                    _replicas;
    opening_replicas            _opening_replicas;
    closing_replicas            _closing_replicas;
    
    mutation_log_ptr            _log;
    std::string                 _dir;
    ::dsn::rpc_address          _primary_address;

    replication_failure_detector *_failure_detector;
    volatile replica_node_state   _state;

    // constants
    replication_options         _options;
    replica_state_subscriber    _replica_state_subscriber;
    bool                        _is_long_subscriber;
    
    // temproal states
    ::dsn::task_ptr _config_query_task;
    ::dsn::task_ptr _config_sync_timer_task;
    ::dsn::task_ptr _gc_timer_task;

    //cli handle, for deregister cli command
    dsn_handle_t    _cli_replica_stub_json_state_handle;

    // performance counters
    perf_counter_    _counter_replicas_count;
    perf_counter_    _counter_replicas_opening_count;
    perf_counter_    _counter_replicas_closing_count;
    perf_counter_    _counter_replicas_total_commit_throught;
    
    perf_counter_    _counter_replicas_learning_failed_latency;
    perf_counter_    _counter_replicas_learning_success_latency;
    perf_counter_    _counter_replicas_learning_count;
private:
    void response_client_error(dsn_message_t request, int error);
};
//------------ inline impl ----------------------

}} // namespace
