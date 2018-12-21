#pragma once

#include <string>
#include <dsn/utility/error_code.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32

enum
{
    FTW_F, /* Regular file.  */
#define FTW_F FTW_F
    FTW_D, /* Directory.  */
#define FTW_D FTW_D
    FTW_DNR, /* Unreadable directory.  */
#define FTW_DNR FTW_DNR
    FTW_NS, /* Unstatable file.  */
#define FTW_NS FTW_NS

    FTW_SL, /* Symbolic link.  */
#define FTW_SL FTW_SL
    /* These flags are only passed from the `nftw' function.  */
    FTW_DP, /* Directory, all subdirs have been visited. */
#define FTW_DP FTW_DP
    FTW_SLN /* Symbolic link naming non-existing file.  */
#define FTW_SLN FTW_SLN
};

struct FTW
{
    int base;
    int level;
};

#else

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif

#include <ftw.h>

#endif

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

#ifdef __cplusplus
}
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
}
}
}
