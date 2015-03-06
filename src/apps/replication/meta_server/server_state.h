#pragma once

#include "replication_common.h"
#include <set>

using namespace rdsn;
using namespace rdsn::service;
using namespace rdsn::replication;

typedef std::list<std::pair<end_point, bool>> NodeStates;

class server_state 
{
public:
    server_state(void);
    ~server_state(void);

    void GetNodeState(__out NodeStates& nodes);
    void SetNodeState(const NodeStates& nodes);
    bool GetMetaServerPrimary(__out end_point& node);

    void AddMetaNode(const end_point& node);
    void RemoveMetaNode(const end_point& node);
    void SwitchMetaPrimary();

    // partition server & client => meta server
    void OnQueryConfig(ConfigurationNodeQueryRequest& request, __out ConfigurationNodeQueryResponse& response);
    void DoQueryConfigurationByIndexRequest(QueryConfigurationByIndexRequest& request, __out QueryConfigurationByIndexResponse& response);
    void update_configuration(configuration_update_request& request, __out ConfigurationUpdateResponse& response);

private:
    void InitApp();
    
private:
    struct AppState
    {
        std::string                  AppType;
        std::string                  AppName;
        int32_t                      AppId;
        int32_t                      PartitionCount;        
        std::vector<partition_configuration>  Partitions;
    };

    struct NodeState
    {
        bool                        IsAlive;
        end_point                 address;
        std::set<global_partition_id, GlobalPartitionIDComparor> Primaries;
        std::set<global_partition_id, GlobalPartitionIDComparor> Partitions;
    };

    zrwlock                          _lock;
    std::map<end_point, NodeState, end_point_comparor> _nodes;
    std::vector<AppState>            _apps;

    zrwlock                          _metaLock;
    std::vector<end_point>         _metaServers;
    int                              _leaderIndex;


    friend class load_balancer;
};

