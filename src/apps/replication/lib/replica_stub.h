#pragma once

//
// the replica_stub is the *singleton* entry to
// access all replica managed in the same process
//   replica_stub(singleton) --> replica --> replication_app
//

#include "replication_common.h"

namespace rdsn { namespace replication {

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
    void close();

    //
    //    requests from clients
    //
    void on_client_write(message_ptr& request);
    void on_client_read(message_ptr& request);

    //
    //    messages from meta server
    //
    void OnConfigProposal(const configuration_update_request& proposal);
    void OnQueryDecree(const QueryPNDecreeRequest& req, __out QueryPNDecreeResponse& resp);
        
    //
    //    messages from peers (primary or secondary)
    //        - prepare
    //        - commit
    //        - learn
    //
    void OnPrepare(message_ptr& request);    
    void OnLearn(const learn_request& request, __out learn_response& response);
    void OnLearnCompletionNotification(const group_check_response& report);
    void OnAddLearner(const group_check_request& request);
    void OnRemove(const replica_configuration& request);
    void OnGroupCheck(const group_check_request& request, __out group_check_response& response);

    //
    //    local messages
    //
    void OnCoordinatorConnected();
    void on_meta_server_disconnected();
    void OnGarbageCollection();

    //
    //  routines published for test
    // 
    void InitGarbageCollectionForTest();
    void SetCoordinatorDisconnectedForTest() { on_meta_server_disconnected(); }
    void SetCoordinatorConnectedForTest(const ConfigurationNodeQueryResponse& config);

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

    void QueryConfiguration();
    void OnCoordinatorDisconnectedScatter(replica_stub_ptr this_, global_partition_id gpid);
    void OnNodeQueryReply(int err, message_ptr& request, message_ptr& response);
    void OnNodeQueryReplyScatter(replica_stub_ptr this_, const partition_configuration& config);
    void OnNodeQueryReplyScatter2(replica_stub_ptr this_, global_partition_id gpid);
    void RemoveReplicaOnCoordinator(const partition_configuration& config);
    task_ptr BeginOpenReplica(const std::string& app_type, global_partition_id gpid, boost::shared_ptr<group_check_request> req = nullptr);
    void    OpenReplica(const std::string app_type, global_partition_id gpid, boost::shared_ptr<group_check_request> req);
    task_ptr BeginCloseReplica(replica_ptr r);
    void CloseReplica(replica_ptr r);
    void AddReplica(replica_ptr r);
    bool RemoveReplica(replica_ptr r);
    void NotifyReplicaStateUpdate(const replica_configuration& config, bool isClosing);

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
    void ResponseClientError(message_ptr& request, int error);
    void replay_mutation(mutation_ptr& mu, Replicas* replicas);
};

DEFINE_REF_OBJECT(replica_stub)

//------------ inline impl ----------------------

}} // namespace
