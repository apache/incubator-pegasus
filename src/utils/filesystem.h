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

#include <string>
#include "utils/error_code.h"

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif

#include <ftw.h>

#ifndef FTW_CONTINUE
#define FTW_CONTINUE 0
#endif

#ifndef FTW_STOP
#define FTW_STOP 1
#endif

#ifndef FTW_SKIP_SUBTREE
#define FTW_SKIP_SUBTREE 2
#endif

#ifndef FTW_SKIP_SIBLINGS
#define FTW_SKIP_SIBLINGS 3
#endif

namespace dsn {
namespace utils {
namespace filesystem {

int get_normalized_path(const std::string &path, std::string &npath);

bool get_absolute_path(const std::string &path1, std::string &path2);

std::string remove_file_name(const std::string &path);

std::string get_file_name(const std::string &path);

std::string path_combine(const std::string &path1, const std::string &path2);

typedef std::function<int(const char *, int, struct FTW *)> ftw_handler;
bool file_tree_walk(const std::string &dirpath, ftw_handler handler, bool recursive = true);

bool path_exists(const std::string &path);

bool directory_exists(const std::string &path);

bool file_exists(const std::string &path);

bool get_subfiles(const std::string &path, std::vector<std::string> &sub_list, bool recursive);

bool get_subdirectories(const std::string &path,
                        std::vector<std::string> &sub_list,
                        bool recursive);

bool get_subpaths(const std::string &path, std::vector<std::string> &sub_list, bool recursive);

// Returns true if no error.
bool remove_path(const std::string &path);

// this will always remove target path if exist
bool rename_path(const std::string &path1, const std::string &path2);

bool file_size(const std::string &path, int64_t &sz);

bool create_directory(const std::string &path);

bool create_file(const std::string &path);

bool get_current_directory(std::string &path);

bool last_write_time(const std::string &path, time_t &tm);

error_code get_process_image_path(int pid, std::string &path);

inline error_code get_current_process_image_path(std::string &path)
{
    auto err = dsn::utils::filesystem::get_process_image_path(-1, path);
    assert(err == ERR_OK);
    return err;
}

struct disk_space_info
{
    // all values are byte counts
    uint64_t capacity;
    uint64_t available;
};
bool get_disk_space_info(const std::string &path, disk_space_info &info);

bool link_file(const std::string &src, const std::string &target);

error_code md5sum(const std::string &file_path, /*out*/ std::string &result);

// return value:
//  - <A, B>:
//          A is represent whether operation encounter some local error
//          B is represent wheter the directory is empty, true means empty, otherwise false
std::pair<error_code, bool> is_directory_empty(const std::string &dirname);

error_code read_file(const std::string &fname, /*out*/ std::string &buf);

// compare file metadata calculated by fname with expected md5 and file_size
bool verify_file(const std::string &fname,
                 const std::string &expected_md5,
                 const int64_t &expected_fsize);

bool verify_file_size(const std::string &fname, const int64_t &expected_fsize);

bool verify_data_md5(const std::string &fname,
                     const char *data,
                     const size_t data_size,
                     const std::string &expected_md5);

// create driectory and get absolute path
bool create_directory(const std::string &path,
                      /*out*/ std::string &absolute_path,
                      /*out*/ std::string &err_msg);

bool write_file(const std::string &fname, std::string &buf);

// check if directory is readable and writable
// call `create_directory` before to make `path` exist
bool check_dir_rw(const std::string &path, /*out*/ std::string &err_msg);

} // namespace filesystem
} // namespace utils
} // namespace dsn
