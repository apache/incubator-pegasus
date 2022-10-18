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

#include "utils/fmt_logging.h"
#include "utils/fail_point.h"

#include "mutation_log_utils.h"

namespace dsn {
namespace replication {
namespace log_utils {

/*extern*/ error_s open_read(string_view path, /*out*/ log_file_ptr &file)
{
    FAIL_POINT_INJECT_F("open_read", [](string_view) -> error_s {
        return error_s::make(ERR_FILE_OPERATION_FAILED, "open_read");
    });

    error_code ec;
    file = log_file::open_read(path.data(), ec);
    if (ec != ERR_OK) {
        return FMT_ERR(ec, "failed to open the log file ({})", path);
    }
    return error_s::ok();
}

/*extern*/ error_s list_all_files(const std::string &dir, /*out*/ std::vector<std::string> &files)
{
    FAIL_POINT_INJECT_F("list_all_files", [](string_view) -> error_s {
        return error_s::make(ERR_FILE_OPERATION_FAILED, "list_all_files");
    });

    if (!utils::filesystem::get_subfiles(dir, files, false)) {
        return FMT_ERR(
            ERR_FILE_OPERATION_FAILED, "unable to list the files under directory ({})", dir);
    }
    return error_s::ok();
}

/*extern*/
error_s check_log_files_continuity(const std::map<int, log_file_ptr> &logs)
{
    if (logs.empty()) {
        return error_s::ok();
    }

    int last_file_index = logs.begin()->first - 1;
    for (const auto &kv : logs) {
        if (++last_file_index != kv.first) {
            // this is a serious error, print all the files in list.
            std::string all_log_files_str;
            bool first = true;
            for (const auto &id_file : logs) {
                if (!first) {
                    all_log_files_str += ", ";
                }
                first = false;
                all_log_files_str += fmt::format(
                    "log.{}.{}", id_file.second->index(), id_file.second->start_offset());
            }

            return FMT_ERR(
                ERR_OBJECT_NOT_FOUND,
                "log file missing with index {}. Here are all the files under dir({}): [{}]",
                last_file_index,
                logs.begin()->second->path(),
                all_log_files_str);
        }
    }
    return error_s::ok();
}

} // namespace log_utils
} // namespace replication
} // namespace dsn
