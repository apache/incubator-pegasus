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
    : replica_base(r_base), _state(collector_state::STOP), _hotkey_type(hotkey_type)
{
    _collector_start_time = dsn_now_s();
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

bool hotkey_collector::start_detect(std::string &err_hint) {}

void hotkey_collector::stop_detect() {}

} // namespace server
} // namespace pegasus
