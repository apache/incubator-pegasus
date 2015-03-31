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

#include "replication_common.h"
#include <set>

using namespace dsn;
using namespace dsn::service;
using namespace dsn::replication;

typedef std::list<std::pair<end_point, bool>> NodeStates;

class server_state 
{
public:
    server_state(void);
    ~server_state(void);

    void GetNodeState(__out_param NodeStates& nodes);
    void SetNodeState(const NodeStates& nodes);
    bool GetMetaServerPrimary(__out_param end_point& node);

    void AddMetaNode(const end_point& node);
    void RemoveMetaNode(const end_point& node);
    void SwitchMetaPrimary();

    // partition server & client => meta server
    void OnQueryConfig(ConfigurationNodeQueryRequest& request, __out_param ConfigurationNodeQueryResponse& response);
    void DoQueryConfigurationByIndexRequest(QueryConfigurationByIndexRequest& request, __out_param QueryConfigurationByIndexResponse& response);
    void update_configuration(configuration_update_request& request, __out_param ConfigurationUpdateResponse& response);

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

