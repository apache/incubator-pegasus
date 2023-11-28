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

#include <stdint.h>
#include <stdio.h>
#include <regex>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "fail_point_impl.h"
#include "utils/fail_point.h"
#include "utils/fmt_logging.h"
#include "utils/rand.h"

namespace dsn {
namespace fail {

static fail_point_registry REGISTRY;

/*extern*/ const std::string *eval(absl::string_view name)
{
    fail_point *p = REGISTRY.try_get(name);
    if (!p) {
        return nullptr;
    }
    return p->eval();
}

inline const char *task_type_to_string(fail_point::task_type t)
{
    switch (t) {
    case fail_point::Off:
        return "Off";
    case fail_point::Return:
        return "Return";
    case fail_point::Print:
        return "Print";
    case fail_point::Void:
        return "Void";
    default:
        LOG_FATAL("unexpected type: {}", t);
        __builtin_unreachable();
    }
}

/*extern*/ void cfg(absl::string_view name, absl::string_view action)
{
    fail_point &p = REGISTRY.create_if_not_exists(name);
    p.set_action(action);
    LOG_INFO("add fail_point [name: {}, task: {}({}), frequency: {}%, max_count: {}]",
             name,
             task_type_to_string(p.get_task()),
             p.get_arg(),
             p.get_frequency(),
             p.get_max_count());
}

/*static*/ bool _S_FAIL_POINT_ENABLED = false;

/*extern*/ void setup() { _S_FAIL_POINT_ENABLED = true; }

/*extern*/ void teardown()
{
    REGISTRY.clear();
    _S_FAIL_POINT_ENABLED = false;
}

void fail_point::set_action(absl::string_view action)
{
    if (!parse_from_string(action)) {
        LOG_FATAL("unrecognized command: {}", action);
    }
}

bool fail_point::parse_from_string(absl::string_view action)
{
    _max_cnt = -1;
    _freq = 100;

    std::regex regex(R"((\d+\%)?(\d+\*)?(\w+)(\((.*)\))?)");
    std::smatch match;

    std::string tmp(action.data(), action.length());
    if (std::regex_match(tmp, match, regex)) {
        if (match.size() == 6) {
            std::ssub_match sub_match = match[1];
            if (!sub_match.str().empty()) {
                sscanf(sub_match.str().data(), "%d%%", &_freq);
            }

            sub_match = match[2];
            if (!sub_match.str().empty()) {
                sscanf(sub_match.str().data(), "%d*", &_max_cnt);
            }

            sub_match = match[3];
            std::string task_type = sub_match.str();
            if (task_type.compare("off") == 0) {
                _task = Off;
            } else if (task_type.compare("return") == 0) {
                _task = Return;
            } else if (task_type.compare("print") == 0) {
                _task = Print;
            } else if (task_type.compare("void") == 0) {
                _task = Void;
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
    LOG_INFO("fail on {}", _name);

    switch (_task) {
    case Off:
        break;
    case Void:
    case Return:
        return &_arg;
    case Print:
        LOG_INFO(_arg);
        break;
    }
    return nullptr;
}

} // namespace fail
} // namespace dsn
