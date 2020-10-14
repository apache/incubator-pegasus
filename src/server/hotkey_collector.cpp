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

#include "base/pegasus_key_schema.h"

namespace pegasus {
namespace server {

// TODO: (Tangyanzhao) implement these functions
void hotkey_collector::handle_rpc(const dsn::replication::detect_hotkey_request &req,
                                  dsn::replication::detect_hotkey_response &resp)
{
}

void hotkey_collector::capture_raw_key(const dsn::blob &raw_key, int64_t weight)
{
    dsn::blob hash_key, sort_key;
    pegasus_restore_key(raw_key, hash_key, sort_key);
    capture_hash_key(hash_key, weight);
}
void hotkey_collector::capture_hash_key(const dsn::blob &hash_key, int64_t weight)
{
    if (collector != nullptr) {
        collector->capture_data(hash_key, weight);
    }
}

} // namespace server
} // namespace pegasus
