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

#include <time.h>

#include <cctype>
#include <cstring>
#include <list>
#include <queue>

#include <boost/lexical_cast.hpp>
#include <rocksdb/slice.h>

#include "runtime/rpc/rpc_address.h"
#include "utils/string_view.h"

namespace pegasus {
namespace utils {

// it's seconds since 2016.01.01-00:00:00 GMT
const uint32_t epoch_begin = 1451606400;
inline uint32_t epoch_now() { return time(nullptr) - epoch_begin; }

// extract "host" from rpc_address
void addr2host(const ::dsn::rpc_address &addr, char *str, int len);

template <typename elem_type, typename compare = std::less<elem_type>>
class top_n
{
public:
    typedef typename std::priority_queue<elem_type, std::vector<elem_type>, compare>
        data_priority_queue;

    top_n(const std::list<elem_type> &data, int n)
    {
        for (const auto &r : data) {
            _queue.emplace(r);
            if (_queue.size() > n) {
                _queue.pop();
            }
        }
    }

    std::list<elem_type> to()
    {
        std::list<elem_type> result;
        while (!_queue.empty()) {
            result.emplace_front(_queue.top());
            _queue.pop();
        }
        return result;
    }

protected:
    data_priority_queue _queue;
};

// ----------------------------------------------------------------------
// c_escape_string()
//    Copies 'src' to 'dest', escaping dangerous characters using
//    '0xFF'-style escape sequences.  'src' and 'dest' should not overlap.
//    Returns the number of bytes written to 'dest' (not including the \0)
//    or (size_t)-1 if there was insufficient space.
// ----------------------------------------------------------------------
size_t c_escape_string(
    const char *src, size_t src_len, char *dest, size_t dest_len, bool always_escape = false);

// T must support data() and length() method.
template <class T>
std::string c_escape_string(const T &src, bool always_escape = false)
{
    const size_t dest_len = src.size() * 4 + 1; // Maximum possible expansion
    char *dest = new char[dest_len];
    const size_t used = c_escape_string(src.data(), src.size(), dest, dest_len, always_escape);
    std::string s(dest, used);
    delete[] dest;
    return s;
}

// ----------------------------------------------------------------------
// c_unescape_string()
//    Copies 'src' to 'dest', unescaping '0xFF'-style escape sequences to
//    original characters.
//    Returns the number of bytes written to 'dest' (not including the \0)
//    or (-n) if unescape failed, where n is the failure position.
// ----------------------------------------------------------------------
int c_unescape_string(const std::string &src, std::string &dest);

inline dsn::string_view to_string_view(rocksdb::Slice s) { return {s.data(), s.size()}; }

inline rocksdb::Slice to_rocksdb_slice(dsn::string_view s) { return {s.data(), s.size()}; }

} // namespace utils
} // namespace pegasus
