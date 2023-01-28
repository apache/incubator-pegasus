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

/*
 * Description:
 *     File system utility functions.
 *
 * Revision history:
 *     2015-08-24, HX Lin(linmajia@live.com), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <fstream>

#include "utils/api_utilities.h"
#include "utils/fmt_logging.h"
#include "utils/defer.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/strings.h"
#include "utils/utils.h"
#include "utils/safe_strerror_posix.h"

#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <boost/filesystem.hpp>
#include <openssl/md5.h>
#include <ftw.h>

#define getcwd_ getcwd
#define rmdir_ rmdir
#define mkdir_(path) mkdir(path, 0775)
#define close_ close
#define stat_ stat

namespace dsn {
namespace utils {
namespace filesystem {

#define _FS_COLON ':'
#define _FS_PERIOD '.'
#define _FS_SLASH '/'
#define _FS_BSLASH '\\'
#define _FS_STAR '*'
#define _FS_QUESTION '?'
#define _FS_NULL '\0'
#define _FS_ISSEP(x) ((x) == _FS_SLASH || (x) == _FS_BSLASH)

static __thread char tls_path_buffer[PATH_MAX];
#define TLS_PATH_BUFFER_SIZE PATH_MAX

// npath need to be a normalized path
static inline int get_stat_internal(const std::string &npath, struct stat_ &st)
{
    int err;

    err = ::stat_(npath.c_str(), &st);
    if (err != 0) {
        err = errno;
    }

    return err;
}

int get_normalized_path(const std::string &path, std::string &npath)
{
    char sep;
    size_t i;
    size_t pos;
    size_t len;
    char c;

    if (path.empty()) {
        npath = "";
        return 0;
    }

    len = path.length();

    sep = _FS_SLASH;
    i = 0;
    pos = 0;
    while (i < len) {
        c = path[i++];
        if (c == _FS_SLASH) {
            while ((i < len) && _FS_ISSEP(path[i])) {
                i++;
            }
        }

        tls_path_buffer[pos++] = c;
    }

    tls_path_buffer[pos] = _FS_NULL;
    if ((c == sep) && (pos > 1)) {
        tls_path_buffer[pos - 1] = _FS_NULL;
    }

    CHECK_NE_MSG(tls_path_buffer[0], _FS_NULL, "Normalized path cannot be empty!");
    npath = tls_path_buffer;

    return 0;
}

static __thread struct
{
    ftw_handler *handler;
    bool recursive;
} tls_ftw_ctx;

static int ftw_wrapper(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
    if (!tls_ftw_ctx.recursive && (ftwbuf->level > 1)) {
        if ((typeflag == FTW_D) || (typeflag == FTW_DP)) {
            return FTW_SKIP_SUBTREE;
        } else {
            return FTW_SKIP_SIBLINGS;
        }
    }

    return (*tls_ftw_ctx.handler)(fpath, typeflag, ftwbuf);
}

bool file_tree_walk(const std::string &dirpath, ftw_handler handler, bool recursive)
{
    tls_ftw_ctx.handler = &handler;
    tls_ftw_ctx.recursive = recursive;
#if defined(__linux__)
    int flags = FTW_ACTIONRETVAL;
#else
    int flags = 0;
#endif // defined(__linux__)
    if (recursive) {
        flags |= FTW_DEPTH;
    }
    int ret = ::nftw(dirpath.c_str(), ftw_wrapper, 1, flags);

    return (ret == 0);
}

// npath need to be a normalized path
static bool path_exists_internal(const std::string &npath, int type)
{
    bool ret;
    struct stat_ st;
    int err;

    err = dsn::utils::filesystem::get_stat_internal(npath, st);
    if (err != 0) {
        return false;
    }

    switch (type) {
    case FTW_F:
        ret = S_ISREG(st.st_mode);
        break;
    case FTW_D:
        ret = S_ISDIR(st.st_mode);
        break;
    case FTW_NS:
        ret = S_ISREG(st.st_mode) || S_ISDIR(st.st_mode);
        break;
    default:
        ret = false;
        break;
    }

    return ret;
}

bool path_exists(const std::string &path)
{
    std::string npath;
    int err;

    if (path.empty()) {
        return false;
    }

    err = get_normalized_path(path, npath);
    if (err != 0) {
        return false;
    }

    return dsn::utils::filesystem::path_exists_internal(npath, FTW_NS);
}

bool directory_exists(const std::string &path)
{
    std::string npath;
    int err;

    if (path.empty()) {
        return false;
    }

    err = get_normalized_path(path, npath);
    if (err != 0) {
        return false;
    }

    return dsn::utils::filesystem::path_exists_internal(npath, FTW_D);
}

bool file_exists(const std::string &path)
{
    std::string npath;
    int err;

    if (path.empty()) {
        return false;
    }

    err = get_normalized_path(path, npath);
    if (err != 0) {
        return false;
    }

    return dsn::utils::filesystem::path_exists_internal(npath, FTW_F);
}

static bool get_subpaths(const std::string &path,
                         std::vector<std::string> &sub_list,
                         bool recursive,
                         int typeflags)
{
    std::string npath;
    bool ret;
    int err;

    if (path.empty()) {
        return false;
    }

    err = get_normalized_path(path, npath);
    if (err != 0) {
        return false;
    }

    if (!dsn::utils::filesystem::path_exists_internal(npath, FTW_D)) {
        return false;
    }

    switch (typeflags) {
    case FTW_F:
        ret = dsn::utils::filesystem::file_tree_walk(
            npath,
            [&sub_list](const char *fpath, int typeflag, struct FTW *ftwbuf) {
                if (typeflag == FTW_F) {
                    sub_list.push_back(fpath);
                }

                return FTW_CONTINUE;
            },
            recursive);
        break;

    case FTW_D:
        ret = dsn::utils::filesystem::file_tree_walk(
            npath,
            [&sub_list](const char *fpath, int typeflag, struct FTW *ftwbuf) {
                if (((typeflag == FTW_D) || (typeflag == FTW_DP)) && (ftwbuf->level > 0)) {
                    sub_list.push_back(fpath);
                }

                return FTW_CONTINUE;
            },
            recursive);
        break;

    case FTW_NS:
        ret = dsn::utils::filesystem::file_tree_walk(
            npath,
            [&sub_list](const char *fpath, int typeflag, struct FTW *ftwbuf) {
                if (ftwbuf->level > 0) {
                    sub_list.push_back(fpath);
                }

                return FTW_CONTINUE;
            },
            recursive);
        break;

    default:
        ret = false;
        break;
    }

    return ret;
}

bool get_subfiles(const std::string &path, std::vector<std::string> &sub_list, bool recursive)
{
    return dsn::utils::filesystem::get_subpaths(path, sub_list, recursive, FTW_F);
}

bool get_subdirectories(const std::string &path, std::vector<std::string> &sub_list, bool recursive)
{
    return dsn::utils::filesystem::get_subpaths(path, sub_list, recursive, FTW_D);
}

bool get_subpaths(const std::string &path, std::vector<std::string> &sub_list, bool recursive)
{
    return dsn::utils::filesystem::get_subpaths(path, sub_list, recursive, FTW_NS);
}

static bool remove_directory(const std::string &npath)
{
    boost::system::error_code ec;
    boost::filesystem::remove_all(npath, ec);
    // TODO(wutao1): return the specific error to caller
    if (dsn_unlikely(bool(ec))) {
        LOG_WARNING("remove {} failed, err = {}", npath, ec.message());
        return false;
    }
    return true;
}

bool remove_path(const std::string &path)
{
    std::string npath;
    int err;

    if (path.empty()) {
        return false;
    }

    err = get_normalized_path(path, npath);
    if (err != 0) {
        return false;
    }

    if (dsn::utils::filesystem::path_exists_internal(npath, FTW_F)) {
        bool ret = (::remove(npath.c_str()) == 0);
        if (!ret) {
            LOG_WARNING("remove file {} failed, err = {}", path, safe_strerror(errno));
        }
        return ret;
    } else if (dsn::utils::filesystem::path_exists_internal(npath, FTW_D)) {
        return dsn::utils::filesystem::remove_directory(npath);
    } else {
        return true;
    }
}

bool rename_path(const std::string &path1, const std::string &path2)
{
    bool ret;

    ret = (::rename(path1.c_str(), path2.c_str()) == 0);
    if (!ret) {
        LOG_WARNING(
            "rename from '{}' to '{}' failed, err = {}", path1, path2, safe_strerror(errno));
    }

    return ret;
}

bool file_size(const std::string &path, int64_t &sz)
{
    struct stat_ st;
    std::string npath;
    int err;

    if (path.empty()) {
        return false;
    }

    err = get_normalized_path(path, npath);
    if (err != 0) {
        return false;
    }

    err = dsn::utils::filesystem::get_stat_internal(npath, st);
    if (err != 0) {
        return false;
    }

    if (!S_ISREG(st.st_mode)) {
        return false;
    }

    sz = st.st_size;

    return true;
}

static int create_directory_component(const std::string &npath)
{
    int err;

    if (::mkdir_(npath.c_str()) == 0) {
        return 0;
    }

    err = errno;
    if (err != EEXIST) {
        return err;
    }

    return (dsn::utils::filesystem::path_exists_internal(npath, FTW_F) ? EEXIST : 0);
}

bool create_directory(const std::string &path)
{
    size_t prev = 0;
    size_t pos;
    char sep;
    std::string npath;
    std::string cpath;
    size_t len;
    int err;

    if (path.empty()) {
        return false;
    }

    err = get_normalized_path(path, npath);
    if (err != 0) {
        return false;
    }

    err = dsn::utils::filesystem::create_directory_component(npath);
    if (err == 0) {
        return true;
    } else if (err != ENOENT) {
        cpath = path;
        goto out_error;
    }

    len = npath.length();
    sep = _FS_SLASH;
    if (npath[0] == sep) {
        prev = 1;
    }

    while ((pos = npath.find_first_of(sep, prev)) != std::string::npos) {
        cpath = npath.substr(0, pos++);
        prev = pos;

        err = dsn::utils::filesystem::create_directory_component(cpath);
        if (err != 0) {
            goto out_error;
        }
    }

    if (prev < len) {
        err = dsn::utils::filesystem::create_directory_component(npath);
        if (err != 0) {
            cpath = npath;
            goto out_error;
        }
    }

    return true;

out_error:
    LOG_WARNING("create_directory {} failed due to cannot create the component: {}, err = {}",
                path,
                cpath,
                safe_strerror(err));
    return false;
}

bool create_file(const std::string &path)
{
    size_t pos;
    std::string npath;
    int fd;
    int mode;
    int err;

    if (path.empty()) {
        return false;
    }

    if (_FS_ISSEP(path.back())) {
        return false;
    }

    err = get_normalized_path(path, npath);
    if (err != 0) {
        return false;
    }

    if (dsn::utils::filesystem::path_exists_internal(npath, FTW_F)) {
        return true;
    }

    if (dsn::utils::filesystem::path_exists_internal(npath, FTW_D)) {
        return false;
    }

    pos = npath.find_last_of("\\/");
    if ((pos != std::string::npos) && (pos > 0)) {
        auto ppath = npath.substr(0, pos);
        if (!dsn::utils::filesystem::create_directory(ppath)) {
            return false;
        }
    }

    mode = 0775;
    fd = ::creat(npath.c_str(), mode);
    if (fd == -1) {
        err = errno;
        LOG_WARNING("create_file {} failed, err = {}", path, safe_strerror(err));
        return false;
    }

    if (::close_(fd) != 0) {
        LOG_WARNING("create_file {}, failed to close the file handle.", path);
    }

    return true;
}

bool get_absolute_path(const std::string &path1, std::string &path2)
{
    bool succ;
    succ = (::realpath(path1.c_str(), tls_path_buffer) != nullptr);
    if (succ) {
        path2 = tls_path_buffer;
    }

    return succ;
}

std::string remove_file_name(const std::string &path)
{
    size_t len;
    size_t pos;

    len = path.length();
    if (len == 0) {
        return "";
    }

    pos = path.find_last_of("\\/");
    if (pos == std::string::npos) {
        return "";
    }

    if (pos == len) {
        return path;
    }

    return path.substr(0, pos);
}

std::string get_file_name(const std::string &path)
{
    size_t len;
    size_t last;
    size_t pos;

    len = path.length();
    if (len == 0) {
        return "";
    }

    last = len - 1;

    pos = path.find_last_of("\\/");

    if (pos == last) {
        return "";
    }

    if (pos == std::string::npos) {
        return path;
    }

    return path.substr((pos + 1), (len - pos));
}

std::string path_combine(const std::string &path1, const std::string &path2)
{
    int err;
    std::string path3;
    std::string npath;

    if (path1.empty()) {
        err = dsn::utils::filesystem::get_normalized_path(path2, npath);
    } else if (path2.empty()) {
        err = dsn::utils::filesystem::get_normalized_path(path1, npath);
    } else {
        path3 = path1;
        path3.append(1, _FS_SLASH);
        path3.append(path2);

        err = dsn::utils::filesystem::get_normalized_path(path3, npath);
    }

    return ((err == 0) ? npath : "");
}

bool get_current_directory(std::string &path)
{
    bool succ;

    succ = (::getcwd_(tls_path_buffer, TLS_PATH_BUFFER_SIZE) != nullptr);
    if (succ) {
        path = tls_path_buffer;
    }

    return succ;
}

bool last_write_time(const std::string &path, time_t &tm)
{
    struct stat_ st;
    std::string npath;
    int err;

    if (path.empty()) {
        return false;
    }

    err = get_normalized_path(path, npath);
    if (err != 0) {
        return false;
    }

    err = dsn::utils::filesystem::get_stat_internal(npath, st);
    if (err != 0) {
        return false;
    }

    tm = st.st_mtime;

    return true;
}

error_code get_process_image_path(int pid, std::string &path)
{
    if (pid < -1) {
        return ERR_INVALID_PARAMETERS;
    }

    int err;

    char tmp[48];

    err = snprintf_p(
        tmp, ARRAYSIZE(tmp), "/proc/%s/exe", (pid == -1) ? "self" : std::to_string(pid).c_str());
    CHECK_GE(err, 0);

    err = (int)readlink(tmp, tls_path_buffer, TLS_PATH_BUFFER_SIZE);
    if (err == -1) {
        return ERR_PATH_NOT_FOUND;
    }

    tls_path_buffer[err] = 0;
    path = tls_path_buffer;

    return ERR_OK;
}

bool get_disk_space_info(const std::string &path, disk_space_info &info)
{
    FAIL_POINT_INJECT_F("filesystem_get_disk_space_info", [&info](string_view str) {
        info.capacity = 100 * 1024 * 1024;
        if (str.find("insufficient") != string_view::npos) {
            info.available = 5 * 1024 * 1024;
        } else {
            info.available = 50 * 1024 * 1024;
        }
        return true;
    });

    boost::system::error_code ec;
    boost::filesystem::space_info in = boost::filesystem::space(path, ec);
    if (ec) {
        LOG_ERROR("get disk space info failed: path = {}, err = {}", path, ec.message());
        return false;
    } else {
        info.capacity = in.capacity;
        info.available = in.available;
        return true;
    }
}

bool link_file(const std::string &src, const std::string &target)
{
    if (src.empty() || target.empty())
        return false;
    if (!file_exists(src) || file_exists(target))
        return false;
    int err = 0;
    err = ::link(src.c_str(), target.c_str());
    return (err == 0);
}

error_code md5sum(const std::string &file_path, /*out*/ std::string &result)
{
    result.clear();
    // if file not exist, we return ERR_OBJECT_NOT_FOUND
    if (!::dsn::utils::filesystem::file_exists(file_path)) {
        LOG_ERROR("md5sum error: file {} not exist", file_path);
        return ERR_OBJECT_NOT_FOUND;
    }

    FILE *fp = fopen(file_path.c_str(), "rb");
    if (fp == nullptr) {
        LOG_ERROR("md5sum error: open file {} failed", file_path);
        return ERR_FILE_OPERATION_FAILED;
    }

    char buf[4096];
    unsigned char out[MD5_DIGEST_LENGTH];
    MD5_CTX c;
    MD5_Init(&c);
    while (true) {
        size_t ret_code = fread(buf, sizeof(char), 4096, fp);
        if (ret_code == 4096) {
            MD5_Update(&c, buf, 4096);
        } else {
            if (feof(fp)) {
                if (ret_code > 0)
                    MD5_Update(&c, buf, ret_code);
                break;
            } else {
                int err = ferror(fp);
                LOG_ERROR("md5sum error: read file {} failed: errno = {} ({})",
                          file_path,
                          err,
                          safe_strerror(err));
                fclose(fp);
                MD5_Final(out, &c);
                return ERR_FILE_OPERATION_FAILED;
            }
        }
    }
    fclose(fp);
    MD5_Final(out, &c);

    char str[MD5_DIGEST_LENGTH * 2 + 1];
    str[MD5_DIGEST_LENGTH * 2] = 0;
    for (int n = 0; n < MD5_DIGEST_LENGTH; n++)
        sprintf(str + n + n, "%02x", out[n]);
    result.assign(str);

    return ERR_OK;
}

std::pair<error_code, bool> is_directory_empty(const std::string &dirname)
{
    std::pair<error_code, bool> res;
    res.first = ERR_OK;
    std::vector<std::string> subfiles;
    std::vector<std::string> subdirs;
    if (get_subfiles(dirname, subfiles, false) && get_subdirectories(dirname, subdirs, false)) {
        res.second = subfiles.empty() && subdirs.empty();
    } else {
        res.first = ERR_FILE_OPERATION_FAILED;
    }
    return res;
}

error_code read_file(const std::string &fname, std::string &buf)
{
    if (!file_exists(fname)) {
        LOG_ERROR("file({}) doesn't exist", fname);
        return ERR_FILE_OPERATION_FAILED;
    }

    int64_t file_sz = 0;
    if (!file_size(fname, file_sz)) {
        LOG_ERROR("get file({}) size failed", fname);
        return ERR_FILE_OPERATION_FAILED;
    }

    buf.resize(file_sz);
    std::ifstream fin(fname, std::ifstream::in);
    if (!fin.is_open()) {
        LOG_ERROR("open file({}) failed", fname);
        return ERR_FILE_OPERATION_FAILED;
    }
    fin.read(&buf[0], file_sz);
    CHECK_EQ_MSG(file_sz,
                 fin.gcount(),
                 "read file({}) failed, file_size = {} but read size = {}",
                 fname,
                 file_sz,
                 fin.gcount());
    fin.close();
    return ERR_OK;
}

bool verify_file(const std::string &fname,
                 const std::string &expected_md5,
                 const int64_t &expected_fsize)
{
    if (!file_exists(fname)) {
        LOG_ERROR("file({}) is not existed", fname);
        return false;
    }
    int64_t f_size = 0;
    if (!file_size(fname, f_size)) {
        LOG_ERROR("verify file({}) failed, becaused failed to get file size", fname);
        return false;
    }
    std::string md5;
    if (md5sum(fname, md5) != ERR_OK) {
        LOG_ERROR("verify file({}) failed, becaused failed to get file md5", fname);
        return false;
    }
    if (f_size != expected_fsize || md5 != expected_md5) {
        LOG_ERROR("verify file({}) failed, because file damaged, size: {} VS {}, md5: {} VS {}",
                  fname,
                  f_size,
                  expected_fsize,
                  md5,
                  expected_md5);
        return false;
    }
    return true;
}

bool verify_file_size(const std::string &fname, const int64_t &expected_fsize)
{
    if (!file_exists(fname)) {
        LOG_ERROR("file({}) is not existed", fname);
        return false;
    }
    int64_t f_size = 0;
    if (!file_size(fname, f_size)) {
        LOG_ERROR("verify file({}) size failed, becaused failed to get file size", fname);
        return false;
    }
    if (f_size != expected_fsize) {
        LOG_ERROR("verify file({}) size failed, because file damaged, size: {} VS {}",
                  fname,
                  f_size,
                  expected_fsize);
        return false;
    }
    return true;
}

bool verify_data_md5(const std::string &fname,
                     const char *data,
                     const size_t data_size,
                     const std::string &expected_md5)
{
    std::string md5 = string_md5(data, data_size);
    if (md5 != expected_md5) {
        LOG_ERROR("verify data({}) failed, because data damaged, size: md5: {} VS {}",
                  fname,
                  md5,
                  expected_md5);
        return false;
    }
    return true;
}

bool create_directory(const std::string &path, std::string &absolute_path, std::string &err_msg)
{
    FAIL_POINT_INJECT_F("filesystem_create_directory", [path](string_view str) {
        // when str contains 'false', and path contains broken_disk_dir, mock create fail(return
        // false)
        std::string broken_disk_dir = "disk1";
        return str.find("false") == string_view::npos ||
               path.find(broken_disk_dir) == std::string::npos;
    });

    if (!create_directory(path)) {
        err_msg = fmt::format("Fail to create directory {}.", path);
        return false;
    }
    if (!get_absolute_path(path, absolute_path)) {
        err_msg = fmt::format("Fail to get absolute path from {}.", path);
        return false;
    }
    return true;
}

bool write_file(const std::string &fname, std::string &buf)
{
    if (!file_exists(fname)) {
        LOG_ERROR("file({}) doesn't exist", fname);
        return false;
    }

    std::ofstream fstream;
    fstream.open(fname.c_str());
    fstream << buf;
    fstream.close();
    return true;
}

bool check_dir_rw(const std::string &path, std::string &err_msg)
{
    FAIL_POINT_INJECT_F("filesystem_check_dir_rw", [path](string_view str) {
        // when str contains 'false', and path contains broken_disk_dir, mock check fail(return
        // false)
        std::string broken_disk_dir = "disk1";
        return str.find("false") == string_view::npos ||
               path.find(broken_disk_dir) == std::string::npos;
    });

    std::string fname = "read_write_test_file";
    std::string fpath = path_combine(path, fname);
    if (!create_file(fpath)) {
        err_msg = fmt::format("Fail to create test file {}.", fpath);
        return false;
    }

    auto cleanup = defer([&fpath]() { remove_path(fpath); });
    std::string value = "test_value";
    if (!write_file(fpath, value)) {
        err_msg = fmt::format("Fail to write file {}.", fpath);
        return false;
    }

    std::string buf;
    if (read_file(fpath, buf) != ERR_OK || buf != value) {
        err_msg = fmt::format("Fail to read file {} or get wrong value({}).", fpath, buf);
        return false;
    }

    return true;
}

} // namespace filesystem
} // namespace utils
} // namespace dsn
