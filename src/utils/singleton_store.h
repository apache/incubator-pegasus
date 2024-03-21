/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include "utils/singleton.h"
#include "utils/synchronize.h"
#include <map>
#include <vector>

namespace dsn {
namespace utils {

template <typename TKey, typename TValue, typename TCompare = std::less<TKey>>
class singleton_store : public dsn::utils::singleton<singleton_store<TKey, TValue, TCompare>>
{
public:
    bool put(TKey key, TValue val)
    {
        auto it = _store.find(key);
        if (it != _store.end())
            return false;
        else {
            _store.insert(std::make_pair(key, val));
            return true;
        }
    }

    bool get(TKey key, /*out*/ TValue &val) const
    {
        auto it = _store.find(key);
        if (it != _store.end()) {
            val = it->second;
            return true;
        } else
            return false;
    }

    bool remove(TKey key) { return _store.erase(key) > 0; }

    void get_all_keys(/*out*/ std::vector<TKey> &keys)
    {
        for (auto it = _store.begin(); it != _store.end(); ++it) {
            keys.push_back(it->first);
        }
    }

private:
    std::map<TKey, TValue, TCompare> _store;
};

template <typename TKey, typename TValue, typename TCompare = std::less<TKey>>
class safe_singleton_store
    : public dsn::utils::singleton<safe_singleton_store<TKey, TValue, TCompare>>
{
public:
    bool put(TKey key, TValue val)
    {
        auto_write_lock l(_lock);
        auto it = _store.find(key);
        if (it != _store.end())
            return false;
        else {
            _store.insert(std::make_pair(key, val));
            return true;
        }
    }

    bool get(TKey key, /*out*/ TValue &val) const
    {
        auto_read_lock l(_lock);
        auto it = _store.find(key);
        if (it != _store.end()) {
            val = it->second;
            return true;
        } else
            return false;
    }

    bool remove(TKey key)
    {
        auto_write_lock l(_lock);
        return _store.erase(key) > 0;
    }

    void get_all_keys(/*out*/ std::vector<TKey> &keys)
    {
        auto_read_lock l(_lock);
        for (auto it = _store.begin(); it != _store.end(); ++it) {
            keys.push_back(it->first);
        }
    }

private:
    std::map<TKey, TValue, TCompare> _store;
    mutable rw_lock_nr _lock;
};

//------------- inline implementation ----------
}
} // end namespace dsn::utils
