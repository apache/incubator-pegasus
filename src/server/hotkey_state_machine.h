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

#include <dsn/utility/enum_helper.h>

namespace pegasus {
namespace server {

//                     hotkey_collector
//                      state machine
//                        +--------+
// data has been cleared, |        |
// ready to start         |  STOP  <-----------+-------------+
//                        |        |           |             |
//                        +---+----+           |             |
//                            +                +             |
//                        Receive START rpc    Time out      |
//                            +                +             |
//                        +---v----+           |             |
//  is running coarse     |        |           |             |
//  capture and analysis  | COARSE +----------->       Receive STOP rpc
//                        |        |           |             |
//                        +---+----+           |             |
//                            +                +             |
//                        Find a hot bucket    Time out      |
//                            +                +             |
//                        +---v----+           |             |
//  is running fine       |        |           |             |
//  capture and analysis  |  FINE  +-----------+             |
//                        |        |                         |
//                        +---+----+                         |
//                            +                              |
//                        Find a hotkey                      |
//                            +                              |
//                        +---v----+                         |
//  capture and analyse   |        |                         |
//  is done, ready to get | FINISH +-------------------------+
//  the result            |        |
//                        +--------+

enum class collector_state
{
    STOP,
    COARSE,
    FINE,
    FINISH
};

ENUM_BEGIN2(collector_state, collector_state, collector_state::STOP)
ENUM_REG(collector_state::STOP)
ENUM_REG(collector_state::COARSE)
ENUM_REG(collector_state::FINE)
ENUM_REG(collector_state::FINISH)
ENUM_END2(collector_state, collector_state)

} // namespace server
} // namespace pegasus
