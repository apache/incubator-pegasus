// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <map>
#include <rocksdb/db.h>
#include <dsn/tool_api.h>
#include <pegasus_const.h>

namespace pegasus {
namespace server {

struct pegasus_scan_context
{
    pegasus_scan_context(std::unique_ptr<rocksdb::Iterator> &&iterator_,
                         const std::string &stop_,
                         bool stop_inclusive_,
                         int32_t batch_size_)
        : _holder(stop_),
          iterator(std::move(iterator_)),
          stop(_holder.data(), _holder.size()),
          stop_inclusive(stop_inclusive_),
          batch_size(batch_size_)
    {
    }

private:
    std::string _holder;

public:
    std::unique_ptr<rocksdb::Iterator> iterator;
    rocksdb::Slice stop;
    bool stop_inclusive;
    int32_t batch_size;
};

class pegasus_context_cache
{
public:
    pegasus_context_cache() : _counter(pegasus::utils::epoch_now() << 20) {}

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
