// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "base/pegasus_value_schema.h"
#include "base/pegasus_utils.h"

#include <cinttypes>
#include <atomic>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/merge_operator.h>

namespace pegasus {
namespace server {

class KeyWithTTLCompactionFilter : public rocksdb::CompactionFilter
{
public:
    KeyWithTTLCompactionFilter() : _value_schema_version(0), _enabled(false) {}
    virtual bool Filter(int /*level*/,
                        const rocksdb::Slice &key,
                        const rocksdb::Slice &existing_value,
                        std::string *new_value,
                        bool *value_changed) const override
    {
        if (!_enabled.load(std::memory_order_acquire))
            return false;
        return check_if_record_expired(
            _value_schema_version, utils::epoch_now(), to_string_view(existing_value));
    }
    virtual const char *Name() const override { return "KeyWithTTLCompactionFilter"; }
    void SetValueSchemaVersion(uint32_t version) { _value_schema_version = version; }
    void EnableFilter() { _enabled.store(true, std::memory_order_release); }
private:
    uint32_t _value_schema_version;
    std::atomic_bool _enabled; // only process filtering when _enabled == true
};

class KeyWithTTLCompactionFilterFactory : public rocksdb::CompactionFilterFactory
{
public:
    KeyWithTTLCompactionFilterFactory() {}
    virtual std::unique_ptr<rocksdb::CompactionFilter>
    CreateCompactionFilter(const rocksdb::CompactionFilter::Context & /*context*/) override
    {
        return std::unique_ptr<KeyWithTTLCompactionFilter>(new KeyWithTTLCompactionFilter());
    }
    virtual const char *Name() const override { return "KeyWithTTLCompactionFilterFactory"; }
};
}
} // namespace
