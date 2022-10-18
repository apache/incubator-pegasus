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

include "../../idl/dsn.thrift"
include "../../idl/dsn.layer2.thrift"
include "metadata.thrift"

namespace cpp dsn.replication

struct mutation_header
{
    1:dsn.gpid             pid;
    2:i64                  ballot;
    3:i64                  decree;
    4:i64                  log_offset;
    5:i64                  last_committed_decree;
    6:i64                  timestamp;
}

struct mutation_update
{
    1:dsn.task_code  code;

    //the serialization type of data, this need to store in log and replicate to secondaries by primary
    2:i32            serialization_type;
    3:dsn.blob       data;
    4:optional i64   start_time_ns;
}

struct mutation_data
{
    1:mutation_header        header;
    2:list<mutation_update>  updates;
}

struct prepare_msg
{
    1:metadata.replica_configuration config;
    2:mutation_data         mu;
}

enum read_semantic
{
    ReadInvalid,
    ReadLastUpdate,
    ReadOutdated,
    ReadSnapshot,
}

struct read_request_header
{
    1:dsn.gpid pid;
    2:dsn.task_code       code;
    3:read_semantic       semantic = read_semantic.ReadLastUpdate;
    4:i64                 version_decree = -1;
}

struct write_request_header
{
    1:dsn.gpid pid;
    2:dsn.task_code       code;
}

struct rw_response_header
{
    1:dsn.error_code      err;
}

struct prepare_ack
{
    1:dsn.gpid pid;
    2:dsn.error_code      err;
    3:i64                 ballot;
    4:i64                 decree;
    5:i64                 last_committed_decree_in_app;
    6:i64                 last_committed_decree_in_prepare_list;
    7:optional i64        receive_timestamp;
    8:optional i64        response_timestamp;
}

enum learn_type
{
    LT_INVALID,
    LT_CACHE,
    LT_APP,
    LT_LOG,
}

struct learn_state
{
    1:i64            from_decree_excluded;
    2:i64            to_decree_included;
    3:dsn.blob       meta;
    4:list<string>   files;

    // Used by duplication. Holds the start_decree of this round of learn.
    5:optional i64   learn_start_decree;
}

enum learner_status
{
    LearningInvalid,
    LearningWithoutPrepare,
    LearningWithPrepareTransient,
    LearningWithPrepare,
    LearningSucceeded,
    LearningFailed,
}

struct learn_request
{
    1:dsn.gpid pid;
    2:dsn.rpc_address     learner; // learner's address
    3:i64                 signature; // learning signature
    4:i64                 last_committed_decree_in_app; // last committed decree of learner's app
    5:i64                 last_committed_decree_in_prepare_list; // last committed decree of learner's prepare list
    6:dsn.blob            app_specific_learn_request; // learning request data by app.prepare_learn_request()

    // Used by duplication to determine if learner has enough logs on disk to
    // be duplicated (ie. max_gced_decree < confirmed_decree), if not,
    // learnee will copy the missing logs.
    7:optional i64        max_gced_decree;
}

struct learn_response
{
    1:dsn.error_code        err; // error code
    2:metadata.replica_configuration config; // learner's replica config
    3:i64                   last_committed_decree; // learnee's last committed decree
    4:i64                   prepare_start_decree; // prepare start decree
    5:learn_type            type = learn_type.LT_INVALID; // learning type: CACHE, LOG, APP
    6:learn_state           state; // learning data, including memory data and files
    7:dsn.rpc_address       address; // learnee's address
    8:string                base_local_dir; // base dir of files on learnee
    9:optional string replica_disk_tag; // the disk tag of learnee located
}

struct learn_notify_response
{
    1:dsn.gpid pid;
    2:dsn.error_code        err; // error code
    3:i64                   signature; // learning signature
}

struct group_check_request
{
    1:dsn.layer2.app_info   app;
    2:dsn.rpc_address       node;
    3:metadata.replica_configuration config;
    4:i64                   last_committed_decree;

    // Used to sync duplication progress between primaries
    // and secondaries, so that secondaries can be allowed to GC
    // their WALs after this decree.
    5:optional i64          confirmed_decree;

    // Used to deliver child gpid and meta_split_status during partition split
    6:optional dsn.gpid     child_gpid;
    7:optional metadata.split_status meta_split_status;
}

struct group_check_response
{
    1:dsn.gpid pid;
    2:dsn.error_code      err;
    3:i64                 last_committed_decree_in_app;
    4:i64                 last_committed_decree_in_prepare_list;
    5:learner_status      learner_status_ = learner_status.LearningInvalid;
    6:i64                 learner_signature;
    7:dsn.rpc_address     node;
    // Used for pause or cancel partition split
    // if secondary pause or cancel split succeed, is_split_stopped = true
    8:optional bool       is_split_stopped;
    9:optional metadata.disk_status disk_status = metadata.disk_status.NORMAL;
}

