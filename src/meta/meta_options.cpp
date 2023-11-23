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

#include "meta_options.h"

#include <stddef.h>
#include <map>
#include <utility>

#include "common/replication_enums.h" // IWYU pragma: keep
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"

namespace dsn {
namespace replication {
// TODO(yingchun): add more description for string configs, and add validators
DSN_DEFINE_string(meta_server,
                  meta_state_service_parameters,
                  "",
                  "meta_state_service provider parameters");
DSN_DEFINE_string(meta_server,
                  meta_function_level_on_start,
                  "steady",
                  "meta function level on start");
DSN_DEFINE_string(meta_server,
                  distributed_lock_service_parameters,
                  "",
                  "distributed_lock_service provider parameters");
DSN_DEFINE_string(meta_server,
                  replica_white_list,
                  "",
                  "white list of replica-servers in meta-server");

std::string meta_options::concat_path_unix_style(const std::string &prefix,
                                                 const std::string &postfix)
{
    size_t pos1 = prefix.size(); // last_valid_pos + 1
    while (pos1 > 0 && prefix[pos1 - 1] == '/')
        pos1--;
    size_t pos2 = 0; // first non '/' position
    while (pos2 < postfix.size() && postfix[pos2] == '/')
        pos2++;
    return prefix.substr(0, pos1) + "/" + postfix.substr(pos2);
}

void meta_options::initialize()
{
    utils::split_args(FLAGS_meta_state_service_parameters, meta_state_service_args);

    meta_function_level_on_start = meta_function_level::fl_invalid;
    std::string level = std::string("fl_") + FLAGS_meta_function_level_on_start;
    for (auto &kv : _meta_function_level_VALUES_TO_NAMES) {
        if (level == kv.second) {
            meta_function_level_on_start = (meta_function_level::type)kv.first;
            break;
        }
    }
    CHECK_NE_MSG(meta_function_level_on_start,
                 meta_function_level::fl_invalid,
                 "invalid function level: {}",
                 FLAGS_meta_function_level_on_start);

    utils::split_args(FLAGS_distributed_lock_service_parameters,
                      _fd_opts.distributed_lock_service_args);
    utils::split_args(FLAGS_replica_white_list, replica_white_list, ',');
}
} // namespace replication
} // namespace dsn
