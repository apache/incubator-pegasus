/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#pragma once

//
// the replica_stub is the *singleton* entry to
// access all replica managed in the same process
//   replica_stub(singleton) --> replica --> replication_app
//

#include "replication_common.h"

namespace dsn { namespace replication {

class mutation_log;
class replication_failure_detector;

// from, new replica config, isClosing
typedef std::function<void (const end_point&, const replica_configuration&, bool)> replica_state_subscriber;

class replica_stub : public serviceletex<replica_stub>, public ref_object
{
public:
    replica_stub(replica_state_subscriber subscriber = nullptr, bool is_long_subscriber = true);
    ~replica_stub(void);

    //
    // initialization
    //
    void initialize(const replication_options& opts, configuration_ptr config, bool clear = false);
    void initialize(configuration_ptr config, bool clear = false);
    void set_options(const replication_options& opts) { _options = opts; }
    void open_service();
    void close();

    //
    //    requests from clients
    //
    void on_client_write(message_ptr& request);
    void on_client_read(message_ptr& request);

    //
    //    messages from meta server
    //
    void on_config_proposal(const configuration_update_request& proposal);
    void on_query_decree(const QueryPNDecreeRequest& req, __out_param QueryPNDecreeResponse& resp);
        
    //
    //    messages from peers (primary or secondary)
    //        - prepare
    //        - commit
    //        - learn
    //
    void on_prepare(message_ptr& request);    
    void on_learn(const learn_request& request, __out_param learn_response& response);
    void on_learn_completion_notification(const group_check_response& report);
    void on_add_learner(const group_check_request& request);
    void on_remove(const replica_configuration& request);
    void on_group_check(const group_check_request& request, __out_param group_check_response& response);

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
    void set_meta_server_connected_for_test(const ConfigurationNodeQueryResponse& config);

    //
    // common routines for inquiry
    //
    const std::string& dir() const { return _dir; }
    replica_ptr get_replica(global_partition_id gpid, bool new_when_possible = false, const char* app_type = nullptr);
    replica_ptr get_replica(int32_t tableId, int32_t partition_index);
    replication_options& options() { return _options; }
    configuration_ptr config() const { return _config; }
    bool is_connected() const { return NS_Connected == _state; }

    // p_tableID = MAX_UInt32 for replica of all tables.
    void get_primary_replica_list(uint32_t p_tableID, std::vector<global_partition_id>& p_repilcaList);

private:    
    enum ReplicaNodeState
    {
        NS_Disconnected,
        NS_Connecting,
        NS_Connected
    };

    void query_configuration();
    void on_meta_server_disconnected_scatter(replica_stub_ptr this_, global_partition_id gpid);
    void on_node_query_reply(int err, message_ptr& request, message_ptr& response);
    void on_node_query_reply_scatter(replica_stub_ptr this_, const partition_configuration& config);
    void on_node_query_reply_scatter2(replica_stub_ptr this_, global_partition_id gpid);
    void remove_replica_on_meta_server(const partition_configuration& config);
    task_ptr begin_open_replica(const std::string& app_type, global_partition_id gpid, std::shared_ptr<group_check_request> req = nullptr);
    void    open_replica(const std::string app_type, global_partition_id gpid, std::shared_ptr<group_check_request> req);
    task_ptr begin_close_replica(replica_ptr r);
    void close_replica(replica_ptr r);
    void add_replica(replica_ptr r);
    bool remove_replica(replica_ptr r);
    void notify_replica_state_update(const replica_configuration& config, bool isClosing);

private:
    typedef std::map<global_partition_id, replica_ptr, GlobalPartitionIDComparor> Replicas;
    typedef std::map<global_partition_id, task_ptr, GlobalPartitionIDComparor> OpeningReplicas;
    typedef std::map<global_partition_id, std::pair<task_ptr, replica_ptr>, GlobalPartitionIDComparor> ClosingReplicas; // <close, replica>
    zlock        _replicasLock;
    Replicas     _replicas;
    OpeningReplicas _openingReplicas;
    ClosingReplicas _closingReplicas;
    
    mutation_log* _log;
    std::string  _dir;

    replication_failure_detector *_livenessMonitor;
    volatile ReplicaNodeState  _state;

    // constants
    replication_options                                  _options;
    configuration_ptr                    _config;
    replica_state_subscriber                              _replicaStateSubscriber;
    bool                                                _isLongSubscriber;
    
    // temproal states
    task_ptr      _partitionConfigurationQueryTask;
    task_ptr      _partitionConfigurationSyncTimerTask;
    task_ptr      _gcTimerTask;

private:    
    friend class replica;
    void response_client_error(message_ptr& request, int error);
    void replay_mutation(mutation_ptr& mu, Replicas* replicas);
};

DEFINE_REF_OBJECT(replica_stub)

//------------ inline impl ----------------------

}} // namespace
