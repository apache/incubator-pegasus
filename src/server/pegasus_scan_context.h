// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <map>
#include <rocksdb/db.h>
#include <dsn/tool_api.h>
#include <dsn/utility/rand.h>
#include <rrdb/rrdb_types.h>

#include "base/pegasus_const.h"
#include "base/pegasus_utils.h"

namespace pegasus {
namespace server {

struct pegasus_scan_context
{
    pegasus_scan_context(std::unique_ptr<rocksdb::Iterator> &&iterator_,
                         const std::string &&stop_,
                         bool stop_inclusive_,
                         ::dsn::apps::filter_type::type hash_key_filter_type_,
                         const std::string &&hash_key_filter_pattern_,
                         ::dsn::apps::filter_type::type sort_key_filter_type_,
                         const std::string &&sort_key_filter_pattern_,
                         int32_t batch_size_,
                         bool no_value_)
        : _stop_holder(std::move(stop_)),
          _hash_key_filter_pattern_holder(std::move(hash_key_filter_pattern_)),
          _sort_key_filter_pattern_holder(std::move(sort_key_filter_pattern_)),
          iterator(std::move(iterator_)),
          stop(_stop_holder.data(), _stop_holder.size()),
          stop_inclusive(stop_inclusive_),
          hash_key_filter_type(hash_key_filter_type_),
          hash_key_filter_pattern(
              _hash_key_filter_pattern_holder.data(), 0, _hash_key_filter_pattern_holder.length()),
          sort_key_filter_type(sort_key_filter_type_),
          sort_key_filter_pattern(
              _sort_key_filter_pattern_holder.data(), 0, _sort_key_filter_pattern_holder.length()),
          batch_size(batch_size_),
          no_value(no_value_)
    {
    }

private:
    std::string _stop_holder;
    std::string _hash_key_filter_pattern_holder;
    std::string _sort_key_filter_pattern_holder;

public:
    std::unique_ptr<rocksdb::Iterator> iterator;
    rocksdb::Slice stop;
    bool stop_inclusive;
    ::dsn::apps::filter_type::type hash_key_filter_type;
    dsn::blob hash_key_filter_pattern;
    ::dsn::apps::filter_type::type sort_key_filter_type;
    dsn::blob sort_key_filter_pattern;
    int32_t batch_size;
    bool no_value;
};

class pegasus_context_cache
{
public:
    pegasus_context_cache()
    {
        // some comments:
        // 1. we should keep the context id unique when the server restarts, so as to prevent
        //    an old scan reuse the context id assigned to a new scan
        // 2. we should prevent the context id mixed when primary switches.
        // 3. we should keep context id positive, as negtive value have specical meanings.
        //
        // a more detailed description on the context id confliction is here:
        //   https://github.com/XiaoMi/pegasus/issues/156
        //
        // however, currently the implementation is not 100% correct.
        //
        _counter = dsn::rand::next_u64(0, 2L << 31);
        _counter <<= 32;
    }

    void clear()
    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);
        _map.clear();
    }

    int64_t put(std::unique_ptr<pegasus_scan_context> context)
    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);
        int64_t handle = _counter++;
        _map[handle] = std::move(context);
        return handle;
    }

    std::unique_ptr<pegasus_scan_context> fetch(int64_t handle)
    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);
        auto kv = _map.find(handle);
        if (kv == _map.end())
            return nullptr;
        std::unique_ptr<pegasus_scan_context> ret = std::move(kv->second);
        _map.erase(kv);
        return ret;
    }

private:
    int64_t _counter;
    std::unordered_map<int64_t, std::unique_ptr<pegasus_scan_context>> _map;
    ::dsn::utils::ex_lock_nr_spin _lock;
};
}
}
