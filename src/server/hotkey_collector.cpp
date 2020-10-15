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

#include "hotkey_collector.h"
#include <dsn/dist/replication/replication_enums.h>

namespace pegasus {
namespace server {

hotkey_collector::hotkey_collector(dsn::replication::hotkey_type::type hotkey_type,
                                   dsn::replication::replica_base *r_base)
    : replica_base(r_base), _state(hotkey_collector_state::STOPPED), _hotkey_type(hotkey_type)
{
}

void hotkey_collector::handle_rpc(const dsn::replication::detect_hotkey_request &req,
                                  dsn::replication::detect_hotkey_response &resp)
{
    if (req.action == dsn::replication::detect_action::START) {
        std::string err_hint;
        if (start_detect(err_hint)) {
            resp.err = dsn::ERR_OK;
        } else {
            resp.err = dsn::ERR_SERVICE_ALREADY_EXIST;
            resp.__set_err_hint(err_hint.c_str());
        }
        return;
    } else {
        stop_detect();
        resp.err = dsn::ERR_OK;
        return;
    }
}

void hotkey_collector::capture_raw_key(const dsn::blob &raw_key, int64_t weight)
{
    // TODO: (Tangyanzhao) Add a judgment sentence to check if it is a raw key
}

void hotkey_collector::capture_hash_key(const dsn::blob &hash_key, int64_t weight) {}

bool hotkey_collector::start_detect(std::string &err_hint)
{
    auto now_state = _state.load();
    switch (_state.load()) {
    case hotkey_collector_state::COARSE_DETECTING:
    case hotkey_collector_state::FINE_DETECTING:
        err_hint = fmt::format("still detecting {} hotkey, state is {}",
                               dsn::enum_to_string(_hotkey_type),
                               enum_to_string(now_state));
        return false;
    case hotkey_collector_state::FINISHED:
        err_hint = fmt::format(
            "{} hotkey result has been found, you can send a stop rpc to restart hotkey detection",
            dsn::enum_to_string(_hotkey_type));
        return false;
    case hotkey_collector_state::STOPPED:
        // TODO: (Tangyanzhao) start coarse detecting
        _state.store(hotkey_collector_state::COARSE_DETECTING);
        ddebug_replica("starting to detect {} hotkey", dsn::enum_to_string(_hotkey_type));
        return true;
    default:
        err_hint = "invalid collector state";
        return false;
    }
}

void hotkey_collector::stop_detect()
{
    _state.store(hotkey_collector_state::STOPPED);
    derror_replica("{} hotkey stopped, cache cleared", dsn::enum_to_string(_hotkey_type));
}

} // namespace server
} // namespace pegasus
