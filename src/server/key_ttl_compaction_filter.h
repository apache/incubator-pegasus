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

#pragma once

#include <cinttypes>
#include <atomic>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/merge_operator.h>
#include <base/pegasus_key_schema.h>
#include <dsn/utility/flags.h>
#include <dsn/perf_counter/perf_counter_wrapper.h>

#include "base/pegasus_utils.h"
#include "base/pegasus_value_schema.h"

namespace pegasus {
namespace server {
inline bool need_clean_key(const rocksdb::Slice &key, uint32_t expire_ts, uint32_t now_ts)
{
    std::vector<std::string> cleaned_hash_key_prefix = {"raw_tts_audio:", "stored_tts_url_info:"};

    uint32_t oneday_sec = 24 * 60 * 60;
    if (expire_ts < now_ts + 3 * oneday_sec) {
        return false;
    }

    std::string hash_key, sort_key;
    pegasus_restore_key(dsn::blob(key.data(), 0, key.size()), hash_key, sort_key);
    for (const auto &key_prefix : cleaned_hash_key_prefix) {
        if (hash_key.length() >= key_prefix.length() &&
            0 == hash_key.compare(0, key_prefix.length(), key_prefix)) {
            return true;
        }
    }

    return false;
}

class KeyWithTTLCompactionFilter : public rocksdb::CompactionFilter
{
public:
    KeyWithTTLCompactionFilter(uint32_t pegasus_data_version, uint32_t default_ttl, bool enabled)
        : _pegasus_data_version(pegasus_data_version), _default_ttl(default_ttl), _enabled(enabled)
    {
        _pfc_cleaned_key_count.init_app_counter("app.pegasus",
                                                "cleaned_key_count",
                                                COUNTER_TYPE_NUMBER,
                                                "statistic the cleaned count of key");
    }

    bool Filter(int /*level*/,
                const rocksdb::Slice &key,
                const rocksdb::Slice &existing_value,
                std::string *new_value,
                bool *value_changed) const override
    {
        if (!_enabled) {
            return false;
        }

        uint32_t expire_ts =
            pegasus_extract_expire_ts(_pegasus_data_version, utils::to_string_view(existing_value));
        if (_default_ttl != 0 && expire_ts == 0) {
            // should update ttl
            *new_value = existing_value.ToString();
            pegasus_update_expire_ts(
                _pegasus_data_version, *new_value, utils::epoch_now() + _default_ttl);
            *value_changed = true;
            return false;
        }

        uint32_t now_ts = utils::epoch_now();
        if (check_if_ts_expired(now_ts, expire_ts)) {
            return true;
        }

        if (need_clean_key(key, expire_ts, now_ts)) {
            _pfc_cleaned_key_count->increment();
            return true;
        }
        return false;
    }

    const char *Name() const override { return "KeyWithTTLCompactionFilter"; }

private:
    uint32_t _pegasus_data_version;
    uint32_t _default_ttl;
    bool _enabled; // only process filtering when _enabled == true
    mutable pegasus_value_generator _gen;

    dsn::perf_counter_wrapper _pfc_cleaned_key_count;
};

class KeyWithTTLCompactionFilterFactory : public rocksdb::CompactionFilterFactory
{
public:
    KeyWithTTLCompactionFilterFactory() : _pegasus_data_version(0), _default_ttl(0), _enabled(false)
    {
    }
    std::unique_ptr<rocksdb::CompactionFilter>
    CreateCompactionFilter(const rocksdb::CompactionFilter::Context & /*context*/) override
    {
        return std::unique_ptr<KeyWithTTLCompactionFilter>(new KeyWithTTLCompactionFilter(
            _pegasus_data_version.load(), _default_ttl.load(), _enabled.load()));
    }
    const char *Name() const override { return "KeyWithTTLCompactionFilterFactory"; }

    void SetPegasusDataVersion(uint32_t version)
    {
        _pegasus_data_version.store(version, std::memory_order_release);
    }
    void EnableFilter() { _enabled.store(true, std::memory_order_release); }
    void SetDefaultTTL(uint32_t ttl) { _default_ttl.store(ttl, std::memory_order_release); }

private:
    std::atomic<uint32_t> _pegasus_data_version;
    std::atomic<uint32_t> _default_ttl;
    std::atomic_bool _enabled; // only process filtering when _enabled == true
};

} // namespace server
} // namespace pegasus
