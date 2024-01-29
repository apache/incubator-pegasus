// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifdef DSN_USE_JEMALLOC

#include "je_ctl.h"

#include <cstring>
#include <vector>

#include <boost/algorithm/string/join.hpp>
#include <jemalloc/jemalloc.h>

#include "utils/api_utilities.h"
#include "utils/fmt_logging.h"

#define RETURN_ARRAY_ELEM_BY_ENUM_TYPE(type, array)                                                \
    do {                                                                                           \
        const auto index = static_cast<size_t>(type);                                              \
        CHECK_LT(index, sizeof(array) / sizeof(array[0]));                                         \
        return array[index];                                                                       \
    } while (0);

namespace dsn {

namespace {

void je_stats_cb(void *opaque, const char *str)
{
    if (str == nullptr) {
        return;
    }

    auto stats = reinterpret_cast<std::string *>(opaque);
    auto avail_capacity = stats->capacity() - stats->size();
    auto len = strlen(str);
    if (len > avail_capacity) {
        len = avail_capacity;
    }

    stats->append(str, len);
}

void je_dump_malloc_stats(const char *opts, size_t buf_sz, std::string &stats)
{
    // Avoid malloc in callback.
    stats.reserve(buf_sz);

    malloc_stats_print(je_stats_cb, &stats, opts);
}

const char *je_stats_type_to_opts(je_stats_type type)
{
    static const char *opts_map[] = {
        "gmdablxe", "mdablxe", "gblxe", "",
    };

    RETURN_ARRAY_ELEM_BY_ENUM_TYPE(type, opts_map);
}

size_t je_stats_type_to_default_buf_sz(je_stats_type type)
{
    static const size_t buf_sz_map[] = {
        2 * 1024, 4 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024,
    };

    RETURN_ARRAY_ELEM_BY_ENUM_TYPE(type, buf_sz_map);
}

} // anonymous namespace

std::string get_all_je_stats_types_str()
{
    std::vector<std::string> names;
    for (size_t i = 0; i < static_cast<size_t>(je_stats_type::COUNT); ++i) {
        names.emplace_back(enum_to_string(static_cast<je_stats_type>(i)));
    }
    return boost::join(names, " | ");
}

void je_dump_stats(je_stats_type type, size_t buf_sz, std::string &stats)
{
    je_dump_malloc_stats(je_stats_type_to_opts(type), buf_sz, stats);
}

void je_dump_stats(je_stats_type type, std::string &stats)
{
    je_dump_stats(type, je_stats_type_to_default_buf_sz(type), stats);
}

} // namespace dsn

#endif // DSN_USE_JEMALLOC
