/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "rocksdb_wrapper.h"

#include "pegasus_write_service_impl.h"

#include <rocksdb/db.h>

namespace pegasus {
namespace server {

rocksdb_wrapper::rocksdb_wrapper(pegasus_server_impl *server,
                                 rocksdb::DB *db,
                                 const uint32_t pegasus_data_version,
                                 rocksdb::ReadOptions &rd_opts,
                                 dsn::perf_counter_wrapper &pfc_recent_expire_count)
    : replica_base(server),
      _db(db),
      _rd_opts(rd_opts),
      _pegasus_data_version(pegasus_data_version),
      _pfc_recent_expire_count(pfc_recent_expire_count)
{
}

int rocksdb_wrapper::get(dsn::string_view raw_key, /*out*/ db_get_context *ctx)
{
    FAIL_POINT_INJECT_F("db_get", [](dsn::string_view) -> int { return FAIL_DB_GET; });

    rocksdb::Status s = _db->Get(_rd_opts, utils::to_rocksdb_slice(raw_key), &(ctx->raw_value));
    if (dsn_likely(s.ok())) {
        // success
        ctx->found = true;
        ctx->expire_ts = pegasus_extract_expire_ts(_pegasus_data_version, ctx->raw_value);
        if (check_if_ts_expired(utils::epoch_now(), ctx->expire_ts)) {
            ctx->expired = true;
            _pfc_recent_expire_count->increment();
        }
        return 0;
    } else if (s.IsNotFound()) {
        // NotFound is an acceptable error
        ctx->found = false;
        return 0;
    }

    dsn::blob hash_key, sort_key;
    pegasus_restore_key(dsn::blob(raw_key.data(), 0, raw_key.size()), hash_key, sort_key);
    derror_rocksdb("Get",
                   s.ToString(),
                   "hash_key: {}, sort_key: {}",
                   utils::c_escape_string(hash_key),
                   utils::c_escape_string(sort_key));
    return s.code();
}

} // namespace server
} // namespace pegasus
