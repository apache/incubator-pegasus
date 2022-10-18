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

// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

/// A fail point implementation in C++.
/// This lib is ported from https://github.com/pingcap/fail-rs.

#include "utils/string_view.h"

/// The only entry to define a fail point with `return` function: lambda function must be
/// return non-void type. When a fail point is defined, it's referenced via the name.
#define FAIL_POINT_INJECT_F(name, lambda)                                                          \
    do {                                                                                           \
        if (dsn_likely(!::dsn::fail::_S_FAIL_POINT_ENABLED))                                       \
            break;                                                                                 \
        auto __Func = lambda;                                                                      \
        auto __Res = ::dsn::fail::eval(name);                                                      \
        if (__Res != nullptr) {                                                                    \
            return __Func(*__Res);                                                                 \
        }                                                                                          \
    } while (0)

/// The only entry to define a fail point with `not return` function: lambda function usually
/// return void type. When a fail point is defined, it's referenced via the name.
#define FAIL_POINT_INJECT_NOT_RETURN_F(name, lambda)                                               \
    do {                                                                                           \
        if (dsn_likely(!::dsn::fail::_S_FAIL_POINT_ENABLED))                                       \
            break;                                                                                 \
        auto __Func = lambda;                                                                      \
        auto __Res = ::dsn::fail::eval(name);                                                      \
        if (__Res != nullptr) {                                                                    \
            __Func(*__Res);                                                                        \
        }                                                                                          \
    } while (0)

namespace dsn {
namespace fail {

extern const std::string *eval(dsn::string_view name);

/// Set new actions to a fail point at runtime.
/// The format of an action is `[p%][cnt*]task[(arg)]`. `p%` is the expected probability that
/// the action is triggered, and `cnt*` is the max times the action can be triggered.
/// For example, `20%3*print(still alive!)` means the fail point has 20% chance to print a
/// message "still alive!". And the message will be printed at most 3 times.
extern void cfg(dsn::string_view name, dsn::string_view action);

extern void setup();

/// Tear down the fail point system.
extern void teardown();

extern bool _S_FAIL_POINT_ENABLED;

} // namespace fail
} // namespace dsn
