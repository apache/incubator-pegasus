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

#include "utils/enum_helper.h"

namespace pegasus {
namespace server {

//                          hotkey_collector
//                               state
// data has been cleared, +----------------+
// ready to start         |    STOPPED     |<--------------+------------+
//                        +-------+--------+               |            |
//                                |                        |            |
//                           Receive START RPC           Timeout        |
//                                |                        +            |
//  is running COARSE     +-------v--------+               |            |
//  capture and analysis  |COARSE_DETECTING+---------------^   Receive STOPPED RPC
//                        +-------+--------+               |            |
//                                |                        +            |
//                           Find a hot bucket           Timeout        |
//                                |                        +            |
//  is running FINE       +-------v--------+               |            |
//  capture and analysis  | FINE_DETECTING |+--------------+            |
//                        +-------+--------+                            |
//                                |                                     |
//                           Find a hotkey                              |
//                                |                                     |
//  capture and analyse   +-------v--------+                            |
//  is done, ready to get |    FINISHED    |+---------------------------+
//  the result            +----------------+

enum class hotkey_collector_state
{
    STOPPED,
    COARSE_DETECTING,
    FINE_DETECTING,
    FINISHED
};

ENUM_BEGIN2(hotkey_collector_state, hotkey_collector_state, hotkey_collector_state::STOPPED)
ENUM_REG(hotkey_collector_state::STOPPED)
ENUM_REG(hotkey_collector_state::COARSE_DETECTING)
ENUM_REG(hotkey_collector_state::FINE_DETECTING)
ENUM_REG(hotkey_collector_state::FINISHED)
ENUM_END2(hotkey_collector_state, hotkey_collector_state)

} // namespace server
} // namespace pegasus
