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

#include "base/pegasus_utils.h"
#include "base/pegasus_key_schema.h"
#include "base/pegasus_value_schema.h"
#include "compaction_operation.h"

namespace pegasus {
namespace server {

class KeyWithTTLCompactionFilter : public rocksdb::CompactionFilter
{
public:
    KeyWithTTLCompactionFilter(uint32_t pegasus_data_version,
                               uint32_t default_ttl,
                               bool enabled,
                               int32_t pidx,
                               int32_t partition_version,
                               bool validate_hash,
                               compaction_operations &&compaction_ops)
        : _pegasus_data_version(pegasus_data_version),
          _default_ttl(default_ttl),
          _enabled(enabled),
          _partition_index(pidx),
          _partition_version(partition_version),
          _validate_partition_hash(validate_hash),
          _user_specified_operations(std::move(compaction_ops))
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

        // ignore empty write. Empty writes will deleted by the compaction of rocksdb. We don't need
        // deal with it here.
        if (key.size() < 2) {
            return false;
        }

        uint32_t expire_ts =
            pegasus_extract_expire_ts(_pegasus_data_version, utils::to_string_view(existing_value));
        if (_default_ttl != 0 && expire_ts == 0) {
            // should update ttl
            expire_ts = utils::epoch_now() + _default_ttl;
            *new_value = existing_value.ToString();
            pegasus_update_expire_ts(_pegasus_data_version, *new_value, expire_ts);
            *value_changed = true;
        }

        if (!_user_specified_operations.empty()) {
            std::string_view value_view = utils::to_string_view(existing_value);
            if (*value_changed) {
                value_view = *new_value;
            }
            if (user_specified_operation_filter(key, value_view, new_value, value_changed)) {
                return true;
            }
        }

        return check_if_ts_expired(utils::epoch_now(), expire_ts) || check_if_stale_split_data(key);
    }

    bool user_specified_operation_filter(const rocksdb::Slice &key,
                                         std::string_view existing_value,
                                         std::string *new_value,
                                         bool *value_changed) const
    {
        std::string hash_key, sort_key;
        pegasus_restore_key(dsn::blob(key.data(), 0, key.size()), hash_key, sort_key);
        for (const auto &op : _user_specified_operations) {
            if (op->filter(hash_key, sort_key, existing_value, new_value, value_changed)) {
                // return true if this data need to be deleted
                return true;
            }
        }
        return false;
    }

    const char *Name() const override { return "KeyWithTTLCompactionFilter"; }

    // Check if the record is stale after partition split, which will split the partition into two
    // halves. The stale record belongs to the other half.
    bool check_if_stale_split_data(const rocksdb::Slice &key) const
    {
        if (!_validate_partition_hash || key.size() < 2 || _partition_version < 0 ||
            _partition_index > _partition_version) {
            return false;
        }
        return !check_pegasus_key_hash(key, _partition_index, _partition_version);
    }

private:
    uint32_t _pegasus_data_version;
    uint32_t _default_ttl;
    bool _enabled; // only process filtering when _enabled == true
    mutable pegasus_value_generator _gen;
    int32_t _partition_index;
    int32_t _partition_version;
    bool _validate_partition_hash;
    compaction_operations _user_specified_operations;
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
        compaction_operations tmp_filter_operations;
        {
            dsn::utils::auto_read_lock l(_lock);
            tmp_filter_operations = _user_specified_operations;
        }

        return std::unique_ptr<KeyWithTTLCompactionFilter>(
            new KeyWithTTLCompactionFilter(_pegasus_data_version.load(),
                                           _default_ttl.load(),
                                           _enabled.load(),
                                           _partition_index.load(),
                                           _partition_version.load(),
                                           _validate_partition_hash.load(),
                                           std::move(tmp_filter_operations)));
    }
    const char *Name() const override { return "KeyWithTTLCompactionFilterFactory"; }

    void SetPegasusDataVersion(uint32_t version)
    {
        _pegasus_data_version.store(version, std::memory_order_release);
    }
    void EnableFilter() { _enabled.store(true, std::memory_order_release); }
    void SetDefaultTTL(uint32_t ttl) { _default_ttl.store(ttl, std::memory_order_release); }
    void SetValidatePartitionHash(bool validate_hash)
    {
        _validate_partition_hash.store(validate_hash, std::memory_order_release);
    }
    void SetPartitionIndex(int32_t pidx)
    {
        _partition_index.store(pidx, std::memory_order_release);
    }
    void SetPartitionVersion(int32_t partition_version)
    {
        _partition_version.store(partition_version, std::memory_order_release);
    }
    void extract_user_specified_ops(const std::string &env)
    {
        auto operations = create_compaction_operations(env, _pegasus_data_version.load());
        {
            dsn::utils::auto_write_lock l(_lock);
            _user_specified_operations.swap(operations);
        }
    }
    void clear_user_specified_ops()
    {
        dsn::utils::auto_write_lock l(_lock);
        _user_specified_operations.clear();
    }

private:
    std::atomic<uint32_t> _pegasus_data_version;
    std::atomic<uint32_t> _default_ttl;
    std::atomic_bool _enabled; // only process filtering when _enabled == true
    std::atomic<int32_t> _partition_index{0};
    std::atomic<int32_t> _partition_version{-1};
    std::atomic_bool _validate_partition_hash{false};

    dsn::utils::rw_lock_nr _lock; // [
    compaction_operations _user_specified_operations;
    // ]
};

} // namespace server
} // namespace pegasus
