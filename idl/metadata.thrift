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

include "dsn.thrift"
include "dsn.layer2.thrift"

namespace cpp dsn.replication
namespace go admin
namespace java org.apache.pegasus.replication

enum partition_status
{
    PS_INVALID,
    PS_INACTIVE,
    PS_ERROR,
    PS_PRIMARY,
    PS_SECONDARY,
    PS_POTENTIAL_SECONDARY,
    PS_PARTITION_SPLIT
}

// partition split status
enum split_status
{
    // idle state
    NOT_SPLIT,
    // A replica is splitting into two replicas, original one called parent, new one called child
    SPLITTING,
    PAUSING,
    PAUSED,
    // After split is successfully cancelled, the state turns into NOT_SPLIT
    CANCELING
}

enum disk_status
{
    NORMAL = 0,
    SPACE_INSUFFICIENT
}

enum manual_compaction_status
{
    IDLE = 0,
    QUEUING,
    RUNNING,
    FINISHED
}

// Used for cold backup and bulk load
struct file_meta
{
    1:string    name;
    2:i64       size;
    3:string    md5;
}

struct replica_configuration
{
    1:dsn.gpid            pid;
    2:i64                 ballot;
    3:dsn.rpc_address     primary;
    4:partition_status    status = partition_status.PS_INVALID;
    5:i64                 learner_signature;
    // Used for bulk load
    // secondary will pop all committed mutations even if buffer is not full
    6:optional bool       pop_all = false;
    // Used for partition split when primary send prepare message to secondary
    // 1. true - secondary should copy mutation in this prepare message synchronously,
    //           and _is_sync_to_child in mutation structure should set true
    // 2. false - secondary copy mutation in this prepare message asynchronously
    // NOTICE: it should always be false when update_local_configuration
    7:optional bool       split_sync_to_child = false;
}

struct replica_info
{
    1:dsn.gpid                          pid;
    2:i64                               ballot;
    3:partition_status                  status;
    4:i64                               last_committed_decree;
    5:i64                               last_prepared_decree;
    6:i64                               last_durable_decree;
    7:string                            app_type;
    8:string                            disk_tag;
    9:optional manual_compaction_status manual_compact_status;
}
