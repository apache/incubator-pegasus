/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

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
// a replica is a replication partition of a serivce,
// which handles all replication related issues
// and redirect the app messages to replication_app_base
// which is binded to this replication partition
//

# include <dsn/serverlet.h>
# include "replication_common.h"
# include "mutation.h"
# include "prepare_list.h"
# include "replica_context.h"

namespace dsn { namespace replication {

class replication_app_base;
class mutation_log;
class replica_stub;

using namespace ::dsn::service;

class replica : public serverlet<replica>, public ref_object
{
public:        
    ~replica(void);

    //
    //    routines for replica stub
    //
    static replica* load(replica_stub* stub, const char* dir, replication_options& options, bool renameDirOnFailure);    
    static replica* newr(replica_stub* stub, const char* app_type, global_partition_id gpid, replication_options& options);    
    void replay_mutation(mutation_ptr& mu);
    void reset_prepare_list_after_replay();
    bool update_local_configuration_with_no_ballot_change(partition_status status);
    void set_inactive_state_transient(bool t);
    void close();

    //
    //    requests from clients
    // 
    void on_client_write(int code, message_ptr& request);
    void on_client_read(const read_request_header& meta, message_ptr& request);

    //
    //    messages and tools from/for meta server
    //
    void on_config_proposal(configuration_update_request& proposal);
    void on_config_sync(const partition_configuration& config);
            
    //
    //    messages from peers (primary or secondary)
    //
    void on_prepare(message_ptr& request);    
    void on_learn(const learn_request& request, __out_param learn_response& response);
    void on_learn_completion_notification(const group_check_response& report);
    void on_add_learner(const group_check_request& request);
    void on_remove(const replica_configuration& request);
    void on_group_check(const group_check_request& request, __out_param group_check_response& response);

    //
    //    messsages from liveness monitor
    //
    void on_meta_server_disconnected();
    
    //
    //  routine for testing purpose only
    //
    void send_group_check_once_for_test(int delay_milliseconds);
    
    //
    //  local information query
    //
    ballot get_ballot() const {return _config.ballot; }    
    partition_status status() const { return _config.status; }
    global_partition_id get_gpid() const { return _config.gpid; }    
    replication_app_base* get_app() { return _app; }
    decree max_prepared_decree() const { return _prepare_list->max_decree(); }
    decree last_committed_decree() const { return _prepare_list->last_committed_decree(); }
    decree last_prepared_decree() const;
    decree last_durable_decree() const;    
    const std::string& dir() const { return _dir; }
    bool group_configuration(__out_param partition_configuration& config) const;
    uint64_t last_config_change_time_milliseconds() const { return _last_config_change_time_ms; }
    const char* name() const { return _name; }
        
private:
    // common helpers
    void init_state();
    void response_client_message(message_ptr& request, int error, decree decree = -1);    
    void execute_mutation(mutation_ptr& mu);
    mutation_ptr new_mutation(decree decree);
    
    // initialization
    int  init_app_and_prepare_list(const char* app_type, bool create_new);
    int  initialize_on_load(const char* dir, bool renameDirOnFailure);        
    int  initialize_on_new(const char* app_type, global_partition_id gpid);
    replica(replica_stub* stub, replication_options& options); // for replica::load(..) only
    replica(replica_stub* stub, global_partition_id gpid, replication_options& options); // for replica::newr(...) only
        
    /////////////////////////////////////////////////////////////////
    // 2pc
    void on_mutation_pending_timeout(mutation_ptr& mu);
    void init_prepare(mutation_ptr& mu);
    void send_prepare_message(const end_point& addr, partition_status status, mutation_ptr& mu, int timeout_milliseconds);
    void on_append_log_completed(mutation_ptr& mu, uint32_t err, uint32_t size);
    void on_prepare_reply(std::pair<mutation_ptr, partition_status> pr, int err, message_ptr& request, message_ptr& reply);
    void do_possible_commit_on_primary(mutation_ptr& mu);    
    void ack_prepare_message(int err, mutation_ptr& mu);
    void cleanup_preparing_mutations(bool isPrimary);
    
    /////////////////////////////////////////////////////////////////
    // learning    
    void init_learn(uint64_t signature);
    void on_learn_reply(error_code err, std::shared_ptr<learn_request>& req, std::shared_ptr<learn_response>& resp);
    void on_learn_remote_state(std::shared_ptr<learn_response> resp);
    void on_learn_remote_state_completed(int err);
    void handle_learning_error(int err);
    void handle_learning_succeeded_on_primary(const end_point& node, uint64_t learnSignature);
    void notify_learn_completion();
        
    /////////////////////////////////////////////////////////////////
    // failure handling    
    void handle_local_failure(int error);
    void handle_remote_failure(partition_status status, const end_point& node, int error);

    /////////////////////////////////////////////////////////////////
    // reconfiguration
    void assign_primary(configuration_update_request& proposal);
    void add_potential_secondary(configuration_update_request& proposal);
    void upgrade_to_secondary_on_primary(const end_point& node);
    void downgrade_to_secondary_on_primary(configuration_update_request& proposal);
    void downgrade_to_inactive_on_primary(configuration_update_request& proposal);
    void remove(configuration_update_request& proposal);
    void update_configuration_on_meta_server(config_type type, const end_point& node, partition_configuration& newConfig);
    void on_update_configuration_on_meta_server_reply(error_code err, message_ptr& request, message_ptr& response, std::shared_ptr<configuration_update_request> req);
    // return if is_closing
    bool update_configuration(const partition_configuration& config);
    bool update_local_configuration(const replica_configuration& config, bool same_ballot = false);
    void replay_prepare_list();
    bool is_same_ballot_status_change_allowed(partition_status olds, partition_status news);

    /////////////////////////////////////////////////////////////////
    // group check
    void init_group_check();
    void broadcast_group_check();
    void on_group_check_reply(error_code err, std::shared_ptr<group_check_request>& req, std::shared_ptr<group_check_response>& resp);
    
private:
    // replica configuration, updated by update_local_configuration ONLY    
    replica_configuration   _config;
    uint64_t                _last_config_change_time_ms;

    // prepare list
    prepare_list*           _prepare_list;

    // application
    replication_app_base*   _app;

    // constants
    replica_stub*           _stub;
    std::string             _dir;
    char                    _name[256]; // TableID.index @ host:port
    replication_options     _options;
    
    // replica status specific states
    primary_context             _primary_states;
    potential_secondary_context _potential_secondary_states;
    bool                        _inactive_is_transient; // upgrade to P/S is allowed only iff true
};

DEFINE_REF_OBJECT(replica)

}} // namespace
