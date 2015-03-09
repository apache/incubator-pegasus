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
#include "replication_common.h"
#include <rdsn/internal/configuration.h>

namespace rdsn { namespace replication {

replication_options::replication_options()
{
    PrepareTimeoutMsForSecondaries = 1000;
    PrepareMaxSendCountForSecondaries = 2;
    LearnTimeoutMs = 30000;
    LearnMaxSendCount = 3;
    StalenessForCommit = 10;
    StalenessForStartPrepareForPotentialSecondary = 110;
    MutationMaxSizeInMB = 15;
    MutationMaxPendingTimeMs = 20;
    MutationApplyMinReplicaNumber = 1;
    PrepareListMaxSizeInMB = 250;
    RequestBatchDisabled = false;
    GroupCheckIntervalMs = 100000;
    GroupCheckMaxSendCount = 3;
    GroupCheckTimeoutMs = 10000;
    GroupCheckDisabled = false;
    GcIntervalMs = 30 * 1000; // 30000 milliseconds
    GcDisabled = false;
    GcMemoryReplicaIntervalMs = 5 * 60 * 1000; // 5 minutes
    GcDiskErrorReplicaIntervalSeconds = 48 * 3600 * 1000; // 48 hrs
    LogBatchWrite = true;
    LogMaxConcurrentWriteLogEntries = 4;
    FD_disabled = false;
    //_options.MetaServers = ...;
    FD_check_interval_seconds = 5;
    FD_beacon_interval_seconds = 3;
    FD_lease_seconds = 10;
    FD_grace_seconds = 15;
    WorkingDir = "test_PartitionServer";
    MachineListFilePath = "../MachineList.csv";
    CoordinatorMachineFunctionName = "KCO";
    CoordinatorPort = 20600;

    CoordinatorRpcCallTimeoutMs = 3000;
    CoordinatorRpcCallMaxSendCount = 10;
        
    LogBufferSizeMB = 1;
    LogPendingMaxMilliseconds = 100;
    LogSegmentFileSizeInMB = 32;
    
    ConfigurationSyncIntervalMs = 30000;
    ConfigurationSyncDisabled = false;

    LearnUsingHttp = false;
    LearnForAdditionalLongSecondsForTest = 0;
}

replication_options::~replication_options()
{
}

void replication_options::ReadMetaServers(configuration_ptr config)
{
    // read MetaServers from machine list file
    MetaServers.clear();

    std::vector<std::string> servers;
    config->get_all_keys("replication.meta_servers", servers);
    for (auto& s : servers)
    {
        // name:port

        int pos1;
        pos1 = s.find_first_of(':');

        if (pos1 != std::string::npos)
        {
            end_point ep(s.substr(0, pos1).c_str(), atoi(s.substr(pos1 + 1).c_str()));
            MetaServers.push_back(ep);
        }
    }
}

void replication_options::initialize(configuration_ptr config)
{
    PrepareTimeoutMsForSecondaries =
        config->get_value<uint32_t>("replication", "PrepareTimeoutMsForSecondaries", PrepareTimeoutMsForSecondaries);
    PrepareMaxSendCountForSecondaries =
        config->get_value<uint32_t>("replication", "PrepareMaxSendCountForSecondaries", PrepareMaxSendCountForSecondaries);
    LearnTimeoutMs =
        config->get_value<uint32_t>("replication", "LearnTimeoutMs", LearnTimeoutMs);
    LearnMaxSendCount =
        config->get_value<uint32_t>("replication", "LearnMaxSendCount", LearnMaxSendCount);
    StalenessForCommit =
        config->get_value<uint32_t>("replication", "StalenessForCommit", StalenessForCommit);
    StalenessForStartPrepareForPotentialSecondary =
        config->get_value<uint32_t>("replication", "StalenessForStartPrepareForPotentialSecondary", StalenessForStartPrepareForPotentialSecondary);
    MutationMaxSizeInMB =
        config->get_value<uint32_t>("replication", "MutationMaxSizeInMB", MutationMaxSizeInMB);
    MutationMaxPendingTimeMs =
        config->get_value<uint32_t>("replication", "MutationMaxPendingTimeMs", MutationMaxPendingTimeMs);
    MutationApplyMinReplicaNumber =
        config->get_value<uint32_t>("replication", "MutationApplyMinReplicaNumber", MutationApplyMinReplicaNumber);
    PrepareListMaxSizeInMB =
        config->get_value<uint32_t>("replication", "PrepareListMaxSizeInMB", PrepareListMaxSizeInMB);
    RequestBatchDisabled =
        config->get_value<bool>("replication", "RequestBatchDisabled", RequestBatchDisabled);
    GroupCheckIntervalMs =
        config->get_value<uint32_t>("replication", "GroupCheckIntervalMs", GroupCheckIntervalMs);
    GroupCheckMaxSendCount =
        config->get_value<uint32_t>("replication", "GroupCheckMaxSendCount", GroupCheckMaxSendCount);
    GroupCheckTimeoutMs =
        config->get_value<uint32_t>("replication", "GroupCheckTimeoutMs", GroupCheckTimeoutMs);
    GroupCheckDisabled =
        config->get_value<bool>("replication", "GroupCheckDisabled", GroupCheckDisabled);
    GcIntervalMs =
        config->get_value<uint32_t>("replication", "GcIntervalMs", GcIntervalMs);
    GcMemoryReplicaIntervalMs =
        config->get_value<uint32_t>("replication", "GcMemoryReplicaIntervalMs", GcMemoryReplicaIntervalMs);
    GcDiskErrorReplicaIntervalSeconds =
        config->get_value<uint32_t>("replication", "GcDiskErrorReplicaIntervalSeconds", GcDiskErrorReplicaIntervalSeconds);
    GcDisabled =
        config->get_value<bool>("replication", "GcDisabled", GcDisabled);

    FD_disabled =
        config->get_value<bool>("replication", "FD_disabled", FD_disabled);
    //_options.MetaServers = ...;
    FD_check_interval_seconds =
        config->get_value<uint32_t>("replication", "FD_check_interval_seconds", FD_check_interval_seconds);
    FD_beacon_interval_seconds =
        config->get_value<uint32_t>("replication", "FD_beacon_interval_seconds", FD_beacon_interval_seconds);
    FD_lease_seconds =
        config->get_value<uint32_t>("replication", "FD_lease_seconds", FD_lease_seconds);
    FD_grace_seconds =
        config->get_value<uint32_t>("replication", "FD_grace_seconds", FD_grace_seconds);
    WorkingDir = config->get_string_value("replication", "WorkingDir", WorkingDir.c_str());

    MachineListFilePath =
        config->get_string_value("replication", "MachineListFilePath", MachineListFilePath.c_str());

    CoordinatorPort =
        config->get_value<uint16_t>("replication", "CoordinatorPort", CoordinatorPort);

    CoordinatorRpcCallTimeoutMs =
        config->get_value<uint32_t>("replication", "CoordinatorRpcCallTimeoutMs", CoordinatorRpcCallTimeoutMs);
    CoordinatorRpcCallMaxSendCount =
        config->get_value<uint32_t>("replication", "CoordinatorRpcCallMaxSendCount", CoordinatorRpcCallMaxSendCount);
        
    LogSegmentFileSizeInMB =
        config->get_value<uint32_t>("replication", "LogSegmentFileSizeInMB", LogSegmentFileSizeInMB);
    LogBufferSizeMB =
        config->get_value<uint32_t>("replication", "LogBufferSizeMB", LogBufferSizeMB);
    LogPendingMaxMilliseconds =
        config->get_value<uint32_t>("replication", "LogPendingMaxMilliseconds", LogPendingMaxMilliseconds);
    LogBatchWrite = 
        config->get_value<bool>("replication", "LogBatchWrite", LogBatchWrite);
    LogMaxConcurrentWriteLogEntries =
        config->get_value<uint32_t>("replication", "LogMaxConcurrentWriteLogEntries", LogMaxConcurrentWriteLogEntries);

     ConfigurationSyncDisabled =
        config->get_value<bool>("replication", "ConfigurationSyncDisabled", ConfigurationSyncDisabled);
    //_options.MetaServers = ...;
    ConfigurationSyncIntervalMs =
        config->get_value<uint32_t>("replication", "ConfigurationSyncIntervalMs", ConfigurationSyncIntervalMs);
        
    LearnUsingHttp = 
        config->get_value<bool>("replication", "LearnUsingHttp", LearnUsingHttp);    
    LearnForAdditionalLongSecondsForTest = 
        config->get_value<uint32_t>("replication", "LearnForAdditionalLongSecondsForTest", LearnForAdditionalLongSecondsForTest);    
            
    ReadMetaServers(config);

    sanity_check();
}

void replication_options::sanity_check()
{
    rassert (GroupCheckTimeoutMs * GroupCheckMaxSendCount < GroupCheckIntervalMs, "");
    rassert(StalenessForStartPrepareForPotentialSecondary >= StalenessForCommit, "");
}
   
/*static*/ bool ReplicaHelper::RemoveNode(const end_point& node, __inout std::vector<end_point>& nodeList)
{
    auto it = std::find(nodeList.begin(), nodeList.end(), node);
    if (it != nodeList.end())
    {
        nodeList.erase(it);
        return true;
    }
    else
    {
        return false;
    }
}

/*static*/ bool ReplicaHelper::GetReplicaConfig(const partition_configuration& partitionConfig, const end_point& node, __out replica_configuration& replicaConfig)
{
    replicaConfig.gpid = partitionConfig.gpid;
    replicaConfig.primary = partitionConfig.primary;
    replicaConfig.ballot = partitionConfig.ballot;

    if (node == partitionConfig.primary)
    {
        replicaConfig.status = PS_PRIMARY;
        return true;
    }
    else if (std::find(partitionConfig.secondaries.begin(), partitionConfig.secondaries.end(), node) != partitionConfig.secondaries.end())
    {
        replicaConfig.status = PS_SECONDARY;
        return true;
    }
    else if (std::find(partitionConfig.dropOuts.begin(), partitionConfig.dropOuts.end(), node) != partitionConfig.dropOuts.end())
    {
        replicaConfig.status = PS_INACTIVE;
        return true;
    }
    else
    {
        replicaConfig.status = PS_INACTIVE;
        return false;
    }
}

}} // end namespace
