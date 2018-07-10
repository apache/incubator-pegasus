// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

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

#include <dsn/utility/string_view.h>

/// The only entry to define a fail point.
/// Add following line to cmake to compile target with fault injection.
/// ```
///   add_definition(-DENABLE_FAIL)
/// ```
/// When a fail point is defined, it's referenced via the name.
#ifdef ENABLE_FAIL
#define FAIL_POINT_INJECT_F(name, lambda)                                                          \
    do {                                                                                           \
        auto __Func = lambda;                                                                      \
        auto __Res = ::dsn::fail::eval(name);                                                      \
        if (__Res != nullptr) {                                                                    \
            return __Func(*__Res);                                                                 \
        }                                                                                          \
    } while (0)
#define FAIL_POINT_INJECT(name) ::dsn::fail::eval(name)
#else
#define FAIL_POINT_INJECT_F(name, lambda)
#define FAIL_POINT_INJECT(name)
#endif

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

} // namespace fail
} // namespace dsn
