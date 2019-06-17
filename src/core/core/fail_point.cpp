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

#include "fail_point_impl.h"

#include <dsn/c/api_layer1.h>
#include <boost/regex.hpp>
#include <dsn/utility/rand.h>

namespace dsn {
namespace fail {

static fail_point_registry REGISTRY;

/*extern*/ const std::string *eval(string_view name)
{
    fail_point *p = REGISTRY.try_get(name);
    if (!p) {
        return nullptr;
    }
    return p->eval();
}

/*extern*/ void cfg(string_view name, string_view action)
{
    fail_point &p = REGISTRY.create_if_not_exists(name);
    p.set_action(action);
}

/*extern*/ void setup() {}

/*extern*/ void teardown() { REGISTRY.clear(); }

void fail_point::set_action(string_view action)
{
    if (!parse_from_string(action)) {
        dfatal("unrecognized command: %s", action.data());
    }
}

bool fail_point::parse_from_string(string_view action)
{
    _max_cnt = -1;
    _freq = 100;

    boost::regex regex(R"((\d+\%)?(\d+\*)?(\w+)(\((.*)\))?)");
    boost::smatch match;

    std::string tmp(action.data(), action.length());
    if (boost::regex_match(tmp, match, regex)) {
        if (match.size() == 6) {
            boost::ssub_match sub_match = match[1];
            if (!sub_match.str().empty()) {
                sscanf(sub_match.str().data(), "%d%%", &_freq);
            }

            sub_match = match[2];
            if (!sub_match.str().empty()) {
                sscanf(sub_match.str().data(), "%d*", &_max_cnt);
            }

            sub_match = match[3];
            string_view task_type = sub_match.str();
            if (task_type.compare("off") == 0) {
                _task = Off;
            } else if (task_type.compare("return") == 0) {
                _task = Return;
            } else if (task_type.compare("print") == 0) {
                _task = Print;
            } else {
                return false;
            }

            sub_match = match[5];
            if (!sub_match.str().empty()) {
                _arg = sub_match.str();
            }

            return true;
        }
    }
    return false;
}

const std::string *fail_point::eval()
{
    uint32_t r = rand::next_u32(0, 100);
    if (r > _freq) {
        return nullptr;
    }
    if (_max_cnt == 0) {
        return nullptr;
    }
    _max_cnt--;

    switch (_task) {
    case Off:
        break;
    case Return:
        return &_arg;
    case Print:
        ddebug(_arg.data());
        break;
    }
    return nullptr;
}

} // namespace fail
} // namespace dsn
