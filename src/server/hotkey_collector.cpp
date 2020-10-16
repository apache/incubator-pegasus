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

namespace pegasus {
namespace server {

hotkey_collector::hotkey_collector(dsn::replication::hotkey_type::type hotkey_type,
                                   dsn::replication::replica_base *r_base)
    : replica_base(r_base),
      _state(hotkey_collector_state::STOPPED),
      _hotkey_type(hotkey_type),
      _internal_collector(std::make_shared<hotkey_empty_data_collector>())
{
}

void hotkey_collector::handle_rpc(const dsn::replication::detect_hotkey_request &req,
                                  dsn::replication::detect_hotkey_response &resp)
{
    switch (req.action) {
    case dsn::replication::detect_action::START:
        if (is_detecting_now()) {
            return_err_resp(resp,
                            fmt::format("still detecting {} hotkey, state is {}",
                                        dsn::enum_to_string(_hotkey_type),
                                        enum_to_string(_state.load())));
        }
        if (is_detecting_finished()) {
            return_err_resp(resp,
                            fmt::format("still detecting {} hotkey, state is {}",
                                        dsn::enum_to_string(_hotkey_type),
                                        enum_to_string(_state.load())));
        }
        if (is_ready_to_detect()) {
            _state.store(hotkey_collector_state::COARSE_DETECTING);
            return_normal_resp(
                resp,
                fmt::format("starting to detect {} hotkey", dsn::enum_to_string(_hotkey_type)));
        }
    case dsn::replication::detect_action::STOP:
        _state.store(hotkey_collector_state::STOPPED);
        _internal_collector.reset();
        return_normal_resp(
            resp,
            fmt::format("{} hotkey stopped, cache cleared", dsn::enum_to_string(_hotkey_type)));
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

void hotkey_collector::analyse_data() { _internal_collector->analyse_data(); }

bool hotkey_collector::is_detecting_now()
{
    return (_state.load() == hotkey_collector_state::COARSE_DETECTING ||
            _state.load() == hotkey_collector_state::FINE_DETECTING);
}

bool hotkey_collector::is_detecting_finished()
{
    return (_state.load() == hotkey_collector_state::FINISHED);
}

bool hotkey_collector::is_ready_to_detect()
{
    return (_state.load() == hotkey_collector_state::STOPPED);
}

void hotkey_collector::return_err_resp(dsn::replication::detect_hotkey_response &resp,
                                       std::string hint)
{
    ddebug_replica(hint);
    resp.err = dsn::ERR_SERVICE_ALREADY_EXIST;
    resp.__set_err_hint(hint.c_str());
}

void hotkey_collector::return_normal_resp(dsn::replication::detect_hotkey_response &resp,
                                          std::string hint)
{
    dinfo_replica(hint);
    resp.err = dsn::ERR_OK;
}

} // namespace server
} // namespace pegasus
