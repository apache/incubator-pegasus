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
#include <dsn/utility/smart_pointers.h>
#include "base/pegasus_key_schema.h"
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/flags.h>

namespace pegasus {
namespace server {

DSN_DEFINE_int32(
    "pegasus.server",
    max_seconds_to_detect_hotkey,
    150,
    "the max time (in seconds) allowed to capture hotkey, will stop if hotkey's not found");

hotkey_collector::hotkey_collector(dsn::replication::hotkey_type::type hotkey_type,
                                   dsn::replication::replica_base *r_base)
    : replica_base(r_base),
      _state(hotkey_collector_state::STOPPED),
      _hotkey_type(hotkey_type),
      _internal_collector(std::make_shared<hotkey_empty_data_collector>()),
      _collector_start_time(0)
{
}

void hotkey_collector::handle_rpc(const dsn::replication::detect_hotkey_request &req,
                                  dsn::replication::detect_hotkey_response &resp)
{
    switch (req.action) {
    case dsn::replication::detect_action::START:
        on_start_detect(resp);
        return;
    case dsn::replication::detect_action::STOP:
        on_stop_detect(resp);
        return;
    default:
        std::string hint = fmt::format("{}: can't find this detect action", req.action);
        resp.err = dsn::ERR_INVALID_STATE;
        resp.__set_err_hint(hint);
        derror_replica(hint);
    }
}

void hotkey_collector::capture_raw_key(const dsn::blob &raw_key, int64_t weight)
{
    dsn::blob hash_key, sort_key;
    pegasus_restore_key(raw_key, hash_key, sort_key);
    capture_hash_key(hash_key, weight);
}

void hotkey_collector::capture_hash_key(const dsn::blob &hash_key, int64_t weight)
{
    // TODO: (Tangyanzhao) add a unit test to ensure data integrity
    _internal_collector->capture_data(hash_key, weight);
}

void hotkey_collector::analyse_data()
{
    switch (_state.load()) {
    case hotkey_collector_state::COARSE_DETECTING:
        terminate_if_timeout();
        _internal_collector->analyse_data();
        return;
    default:
        return;
    }
}

void hotkey_collector::on_start_detect(dsn::replication::detect_hotkey_response &resp)
{
    auto now_state = _state.load();
    std::string hint;
    switch (now_state) {
    case hotkey_collector_state::COARSE_DETECTING:
    case hotkey_collector_state::FINE_DETECTING:
        resp.err = dsn::ERR_INVALID_STATE;
        hint = fmt::format("still detecting {} hotkey, state is {}",
                           dsn::enum_to_string(_hotkey_type),
                           enum_to_string(now_state));
        dwarn_replica(hint);
        return;
    case hotkey_collector_state::FINISHED:
        resp.err = dsn::ERR_INVALID_STATE;
        hint = fmt::format("{} hotkey result has been found, you can send a stop rpc to "
                           "restart hotkey detection",
                           dsn::enum_to_string(_hotkey_type));
        dwarn_replica(hint);
        return;
    case hotkey_collector_state::STOPPED:
        _collector_start_time = dsn_now_s();
        // TODO: (Tangyanzhao) start coarse detecting
        _state.store(hotkey_collector_state::COARSE_DETECTING);
        resp.err = dsn::ERR_OK;
        hint = fmt::format("starting to detect {} hotkey", dsn::enum_to_string(_hotkey_type));
        ddebug_replica(hint);
        return;
    default:
        hint = "invalid collector state";
        resp.err = dsn::ERR_INVALID_STATE;
        resp.__set_err_hint(hint);
        derror_replica(hint);
        dassert(false, "invalid collector state");
    }
}

void hotkey_collector::on_stop_detect(dsn::replication::detect_hotkey_response &resp)
{
    terminate();
    resp.err = dsn::ERR_OK;
    std::string hint =
        fmt::format("{} hotkey stopped, cache cleared", dsn::enum_to_string(_hotkey_type));
    ddebug_replica(hint);
}

void hotkey_collector::terminate()
{
    _state.store(hotkey_collector_state::STOPPED);
    _internal_collector.reset();
    _collector_start_time = 0;
}

void hotkey_collector::terminate_if_timeout()
{
    if (_collector_start_time == 0) {
        return;
    }
    if (dsn_now_s() >= _collector_start_time + FLAGS_max_seconds_to_detect_hotkey) {
        ddebug_replica("hotkey collector work time is exhausted but no hotkey has been found");
        terminate();
        return;
    }
};

} // namespace server
} // namespace pegasus
