// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <dsn/utility/string_view.h>
#include <dsn/dist/replication/replication_types.h>
#include "hotkey_collector_state.h"

namespace pegasus {
namespace server {

// TODO: (Tangyanzhao) detect_result::hot_bucket_index should be -1 when hotkey_collector construct
struct detect_result
{
    int hot_bucket_index;
    std::string hotkey;
};

class internal_collector_base
{
public:
    virtual void capture_data(const dsn::blob &hash_key, uint64_t weight);
    virtual void analyse_data();
    virtual bool get_analysis_result(/*out*/ detect_result &result);
};

//    hotkey_collector is responsible to find the hot keys after the partition
//    was detected to be hot. The two types of hotkey, READ & WRITE, are detected
//    separately.
//
//    +--------------------+  +----------------------------------------------------+
//    |   Replcia server   |  | Hotkey collector                                   |
//    |                    |  | +-----------------------------------------------+  |
//    | +----------------+ |  | | Corase capture                                |  |
//    | |                | |--> |                 +----------+                  |  |
//    | |  RPC received  | || | |                 |   Data   |                  |  |
//    | |                | || | |                 +-----+----+                  |  |
//    | +-------+--------+ || | |                       |                       |  |
//    |         |          || | |  +---------------+----v--+-------+---------+  |  |
//    |         v          || | |  |       |Hot    |       |       |         |  |  |
//    | +-------+--------+ || | |  |Bucket |Bucket |Bucket |Bucket |Bucket   |  |  |
//    | |   Replication  | || | |  +-----------+-----------------------------+  |  |
//    | | (only on the   | || | |              |                                |  |
//    | |  write path))  | || | +--------------|--------------------------------+  |
//    | +-------+--------+ || |             +--v---+                               |
//    |         |          || |             | Data |                               |
//    |         v          || |             +------+                               |
//    | +-------+--------+ || |          +-----|-------+-------------+             |
//    | |                | || |   +------|-------------|-------------|---------+   |
//    | |  Capture data  ---| |   | Fine |capture      |             |         |   |
//    | |                | |  |   |      |             |             |         |   |
//    | +-------+--------+ |  |   | +----v----+   +----v----+   +----v----+    |   |
//    |         |          |  |   | |  queue  |   |  queue  |   |  queue  |    |   |
//    |         v          |  |   | +----+----+   +----+----+   +----+----+    |   |
//    | +-------+--------+ |  |   |      |             |             |         |   |
//    | |                | |  |   | +----v-------------v-------------v------+  |   |
//    | |   Place data   | |  |   | |             Analsis pool              |  |   |
//    | |   to the disk  | |  |   | +-----------------|---------------------+  |   |
//    | |                | |  |   +-------------------|------------------------+   |
//    | +----------------+ |  |                       v                            |
//    |                    |  |                     Hotkey                         |
//    +--------------------+  +----------------------------------------------------+

class hotkey_collector
{
public:
    // TODO: (Tangyanzhao) capture_*_key should be consistent with hotspot detection
    // weight: calculate the weight according to the specific situation
    void capture_raw_key(const dsn::blob &raw_key, int64_t weight);
    void capture_hash_key(const dsn::blob &hash_key, int64_t weight);
    void handle_rpc(const dsn::replication::detect_hotkey_request &req,
                    /*out*/ dsn::replication::detect_hotkey_response &resp);

private:
    detect_result _result;
    std::unique_ptr<internal_collector_base> collector;
    std::atomic<hotkey_collector_state> _state;
};

class hotkey_coarse_data_collector : public internal_collector_base
{
public:
    void capture_data(const dsn::blob &hash_key, uint64_t size);
    void analyse_data();
    virtual bool get_analysis_result(/*out*/ detect_result &result);
};

class hotkey_fine_data_collector : public internal_collector_base
{
public:
    void capture_data(const dsn::blob &hash_key, uint64_t size);
    void analyse_data();
    virtual bool get_analysis_result(/*out*/ detect_result &result);
};

class hotkey_empty_data_collector : public internal_collector_base
{
public:
    void capture_data(const dsn::blob &hash_key, uint64_t size);
    void analyse_data();
    virtual bool get_analysis_result(/*out*/ detect_result &result);
};

} // namespace server
} // namespace pegasus
