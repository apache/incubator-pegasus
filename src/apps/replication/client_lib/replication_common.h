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
# pragma once

# include <dsn/serverlet.h>
# include <string>
# include "codes.h"
# include "replication_ds.h"

using namespace ::dsn::service;

namespace dsn { namespace replication {

typedef int32_t app_id;
typedef int32_t PartitionID;
typedef int64_t ballot;
typedef int64_t decree;

#define invalid_ballot ((ballot)-1LL)
#define invalid_decree ((decree)-1LL)
#define invalid_offset (-1LL)

class replica_stub;
typedef boost::intrusive_ptr<replica_stub> replica_stub_ptr;

class replica;
typedef boost::intrusive_ptr<replica> replica_ptr;

class mutation;
typedef boost::intrusive_ptr<mutation> mutation_ptr;

struct GlobalPartitionIDComparor
{
  bool operator()(const global_partition_id& s1, const global_partition_id& s2) const
  {
    return s1.tableId < s2.tableId
        || (s1.tableId == s2.tableId && s1.pidx < s2.pidx);
  }
};

inline int gpid_to_hash(global_partition_id gpid)
{
    return static_cast<int>(gpid.tableId ^ gpid.pidx);
}

typedef std::set<end_point, end_point_comparor> NodeSet;
typedef std::map<end_point, int, end_point_comparor> NodeIdMap;
typedef std::map<end_point, decree, end_point_comparor> NodeDecreeMap;
typedef std::map<end_point, partition_status, end_point_comparor> NodeStatusMap;
typedef std::map<end_point, task_ptr, end_point_comparor> node_tasks;

class replication_options
{
public:
    std::string MachineListFilePath;
    std::string CoordinatorMachineFunctionName;
    std::string WorkingDir;
    uint16_t CoordinatorPort;

    int32_t CoordinatorRpcCallTimeoutMs;
    int32_t CoordinatorRpcCallMaxSendCount;
    int32_t PrepareTimeoutMsForSecondaries;
    int32_t PrepareMaxSendCountForSecondaries;
    int32_t GroupCheckTimeoutMs;
    int32_t GroupCheckMaxSendCount;
    int32_t GroupCheckIntervalMs;
    int32_t LearnTimeoutMs;
    int32_t LearnMaxSendCount;
    int32_t PrepareListMaxSizeInMB;
    int32_t StalenessForCommit;
    int32_t StalenessForStartPrepareForPotentialSecondary;
    int32_t MutationMaxSizeInMB;
    int32_t MutationMaxPendingTimeMs;
    int32_t MutationApplyMinReplicaNumber;
    bool RequestBatchDisabled;
    bool GroupCheckDisabled;
    int32_t GcIntervalMs;
    bool GcDisabled;
    int32_t GcMemoryReplicaIntervalMs;
    int32_t GcDiskErrorReplicaIntervalSeconds;
    bool FD_disabled;
    std::vector<end_point> MetaServers;
    int32_t FD_check_interval_seconds;
    int32_t FD_beacon_interval_seconds;
    int32_t FD_lease_seconds;
    int32_t FD_grace_seconds;

    int32_t LogSegmentFileSizeInMB;
    int32_t LogBufferSizeMB;
    int32_t LogPendingMaxMilliseconds;
    bool    LogBatchWrite;
    int32_t LogMaxConcurrentWriteLogEntries;
    int32_t ConfigurationSyncIntervalMs;
    bool    ConfigurationSyncDisabled;
    bool    LearnUsingHttp; 
    int32_t LearnForAdditionalLongSecondsForTest;
    
public:
    replication_options();
    void initialize(configuration_ptr config);
    ~replication_options();

private:
    void ReadMetaServers(configuration_ptr config);
    void sanity_check();
};

class ReplicaHelper
{
public:
    static bool RemoveNode(const end_point& node, __inout_param std::vector<end_point>& nodeList);
    static bool GetReplicaConfig(const partition_configuration& partitionConfig, const end_point& node, __out_param replica_configuration& replicaConfig);
};

}} // namespace
