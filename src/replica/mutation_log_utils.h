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

#include "utils/errors.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"

#include "mutation_log.h"

namespace dsn {
namespace replication {
namespace log_utils {

extern error_s open_read(string_view path, /*out*/ log_file_ptr &file);

extern error_s list_all_files(const std::string &dir, /*out*/ std::vector<std::string> &files);

inline error_s open_log_file_map(const std::vector<std::string> &log_files,
                                 /*out*/ std::map<int, log_file_ptr> &log_file_map)
{
    for (const std::string &fname : log_files) {
        log_file_ptr lf;
        error_s err = open_read(fname, lf);
        if (!err.is_ok()) {
            return err << "open_log_file_map(log_files)";
        }
        log_file_map[lf->index()] = lf;
    }
    return error_s::ok();
}

inline error_s open_log_file_map(const std::string &dir,
                                 /*out*/ std::map<int, log_file_ptr> &log_file_map)
{
    std::vector<std::string> log_files;
    error_s es = list_all_files(dir, log_files);
    if (!es.is_ok()) {
        return es << "open_log_file_map(dir)";
    }
    return open_log_file_map(log_files, log_file_map) << "open_log_file_map(dir)";
}

extern error_s check_log_files_continuity(const std::map<int, log_file_ptr> &logs);

inline error_s check_log_files_continuity(const std::string &dir)
{
    std::map<int, log_file_ptr> log_file_map;
    error_s es = open_log_file_map(dir, log_file_map);
    if (!es.is_ok()) {
        return es << "check_log_files_continuity(dir)";
    }
    return check_log_files_continuity(log_file_map) << "check_log_files_continuity(dir)";
}

} // namespace log_utils
} // namespace replication
} // namespace dsn
