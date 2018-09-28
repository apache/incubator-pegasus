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

#include <dsn/dist/fmt_logging.h>
#include <dsn/cpp/rpc_holder.h>
#include <dsn/cpp/json_helper.h>
#include <dsn/utility/string_conv.h>

namespace dsn {
namespace replication {

typedef rpc_holder<duplication_status_change_request, duplication_status_change_response>
    duplication_status_change_rpc;
typedef rpc_holder<duplication_add_request, duplication_add_response> duplication_add_rpc;
typedef rpc_holder<duplication_query_request, duplication_query_response> duplication_query_rpc;
typedef rpc_holder<duplication_sync_request, duplication_sync_response> duplication_sync_rpc;

typedef int32_t dupid_t;

inline bool convert_str_to_dupid(string_view str, dupid_t *dupid) { return buf2int32(str, *dupid); }

inline const char *duplication_status_to_string(const duplication_status::type &status)
{
    auto it = _duplication_status_VALUES_TO_NAMES.find(status);
    dassert(it != _duplication_status_VALUES_TO_NAMES.end(),
            "unexpected type of duplication_status: %d",
            status);
    return it->second;
}

inline void json_encode(dsn::json::JsonWriter &out, const duplication_status::type &s)
{
    json_encode(out, duplication_status_to_string(s));
}

inline bool json_decode(const dsn::json::JsonObject &in, duplication_status::type &s)
{
    static const std::map<std::string, duplication_status::type>
        _duplication_status_NAMES_TO_VALUES = {
            {"DS_INIT", duplication_status::DS_INIT},
            {"DS_PAUSE", duplication_status::DS_PAUSE},
            {"DS_START", duplication_status::DS_START},
            {"DS_REMOVED", duplication_status::DS_REMOVED},
        };

    std::string name;
    json_decode(in, name);
    auto it = _duplication_status_NAMES_TO_VALUES.find(name);
    if (it != _duplication_status_NAMES_TO_VALUES.end()) {
        s = it->second;
        return true;
    }
    dfatal_f("unexpected duplication_status name: {}", name);
    __builtin_unreachable();
}

} // namespace replication
} // namespace dsn
