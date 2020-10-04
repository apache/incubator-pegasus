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

namespace pegasus {
namespace server {

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
    // size: the cu size of raw_key/hash_key calculated by `capacity_unit_calculator`
    void capture_raw_key(const dsn::blob &raw_key, uint64_t size);
    void handle_operation(const dsn::replication::detect_hotkey_request &req,
                          dsn::replication::detect_hotkey_response &resp);
};

} // namespace server
} // namespace pegasus
