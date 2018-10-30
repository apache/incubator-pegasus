// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <cinttypes>
#include <atomic>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/merge_operator.h>

#include "base/pegasus_utils.h"
#include "base/pegasus_value_schema.h"

namespace pegasus {
namespace server {

class KeyWithTTLCompactionFilter : public rocksdb::CompactionFilter
{
public:
    KeyWithTTLCompactionFilter(uint32_t value_schema_version, uint32_t default_ttl, bool enabled)
        : _value_schema_version(value_schema_version), _default_ttl(default_ttl), _enabled(enabled)
    {
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

        uint32_t expire_ts = pegasus_extract_expire_ts(
            _value_schema_version,
            dsn::string_view(existing_value.data(), existing_value.length()));
        if (_default_ttl != 0 && expire_ts == 0) {
            // should update ttl
            dsn::blob user_data;
            pegasus_extract_user_data(_value_schema_version,
                                      std::string(existing_value.data(), existing_value.length()),
                                      user_data);
            rocksdb::SliceParts sparts = _gen.generate_value(_value_schema_version,
                                                             dsn::string_view(user_data),
                                                             utils::epoch_now() + _default_ttl);
            for (int i = 0; i < sparts.num_parts; i++) {
                *new_value += sparts.parts[i].ToString();
            }
            *value_changed = true;
            return false;
        }
        return check_if_ts_expired(utils::epoch_now(), expire_ts);
    }

    const char *Name() const override { return "KeyWithTTLCompactionFilter"; }

private:
    uint32_t _value_schema_version;
    uint32_t _default_ttl;
    bool _enabled; // only process filtering when _enabled == true
    mutable pegasus_value_generator _gen;
};

class KeyWithTTLCompactionFilterFactory : public rocksdb::CompactionFilterFactory
{
public:
    KeyWithTTLCompactionFilterFactory() : _value_schema_version(0), _default_ttl(0), _enabled(false)
    {
    }
    std::unique_ptr<rocksdb::CompactionFilter>
    CreateCompactionFilter(const rocksdb::CompactionFilter::Context & /*context*/) override
    {
        return std::unique_ptr<KeyWithTTLCompactionFilter>(new KeyWithTTLCompactionFilter(
            _value_schema_version, _default_ttl.load(), _enabled.load()));
    }
    const char *Name() const override { return "KeyWithTTLCompactionFilterFactory"; }

    void SetValueSchemaVersion(uint32_t version) { _value_schema_version = version; }
    void EnableFilter() { _enabled.store(true, std::memory_order_release); }
    void set_default_ttl(uint32_t ttl) { _default_ttl.store(ttl, std::memory_order_release); }

private:
    uint32_t _value_schema_version;
    std::atomic<uint32_t> _default_ttl;
    std::atomic_bool _enabled; // only process filtering when _enabled == true
};

} // server
} // pegasus
