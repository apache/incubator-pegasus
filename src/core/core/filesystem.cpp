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

#include <dsn/utility/filesystem.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdio.h>
#include <boost/filesystem.hpp>

#ifdef _WIN32

#include <direct.h>
#include <io.h>
#include <deque>

#define getcwd_ _getcwd
#define rmdir_ _rmdir
#define mkdir_ _mkdir
#define close_ _close
#define stat_ _stat64

#ifndef __S_ISTYPE
#define __S_ISTYPE(mode, mask) (((mode)&S_IFMT) == (mask))
#endif

#ifndef S_ISREG
#define S_ISREG(mode) __S_ISTYPE((mode), S_IFREG)
#endif

#ifndef S_ISDIR
#define S_ISDIR(mode) __S_ISTYPE((mode), S_IFDIR)
#endif

#ifndef PATH_MAX
#define PATH_MAX MAX_PATH
#endif

#else

#include <openssl/md5.h>

#if defined(__FreeBSD__)
#include <sys/types.h>
#include <sys/user.h>
#include <libutil.h>
#endif

#if defined(__APPLE__)
#include <libproc.h>
#endif

#define getcwd_ getcwd
#define rmdir_ rmdir
#define mkdir_(path) mkdir(path, 0775)
#define close_ close
#define stat_ stat

#endif

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "dsn.file_utils"

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
        /*
        dinfo("get_stat_internal %s failed, err = %s",
            npath.c_str(),
            strerror(err)
            );
        */
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

#ifdef _WIN32
    sep = _FS_BSLASH;
#else
    sep = _FS_SLASH;
#endif
    i = 0;
    pos = 0;
    while (i < len) {
        c = path[i++];
        if (
#ifdef _WIN32
            _FS_ISSEP(c)
#else
            c == _FS_SLASH
#endif
                ) {
#ifdef _WIN32
            c = sep;
            if (i > 1)
#endif
                while ((i < len) && _FS_ISSEP(path[i])) {
                    i++;
                }
        }

        tls_path_buffer[pos++] = c;
    }

    tls_path_buffer[pos] = _FS_NULL;
    if ((c == sep) && (pos > 1)) {
#ifdef _WIN32
        c = tls_path_buffer[pos - 2];
        if ((c != _FS_COLON) && (c != _FS_QUESTION) && (c != _FS_BSLASH))
#endif
            tls_path_buffer[pos - 1] = _FS_NULL;
    }

    dassert(tls_path_buffer[0] != _FS_NULL, "Normalized path cannot be empty!");
    npath = tls_path_buffer;

    return 0;
}

#ifndef _WIN32
static __thread struct
{
    ftw_handler *handler;
    bool recursive;
} tls_ftw_ctx;

static int ftw_wrapper(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
    if (!tls_ftw_ctx.recursive && (ftwbuf->level > 1)) {
#ifdef __linux__
        if ((typeflag == FTW_D) || (typeflag == FTW_DP)) {
            return FTW_SKIP_SUBTREE;
        } else {
            return FTW_SKIP_SIBLINGS;
        }
#else
        return 0;
#endif
    }

    return (*tls_ftw_ctx.handler)(fpath, typeflag, ftwbuf);
}
#endif

#ifdef _WIN32
struct win_ftw_info
{
    struct FTW ftw;
    std::string path;
};
#endif

bool file_tree_walk(const std::string &dirpath, ftw_handler handler, bool recursive)
{
#ifdef _WIN32
    WIN32_FIND_DATAA ffd;
    HANDLE hFind;
    DWORD dwError = ERROR_SUCCESS;
    std::deque<win_ftw_info> queue;
    std::deque<win_ftw_info> queue2;
    char c;
    size_t pos;
    win_ftw_info info;
    win_ftw_info info2;
    int ret;
    int err;

    info.path.reserve(PATH_MAX);
    err = dsn::utils::filesystem::get_normalized_path(dirpath, info.path);
    if (err != 0) {
        return false;
    }
    info2.path.reserve(PATH_MAX);
    pos = info.path.find_last_of("\\");
    info.ftw.base = (info.ftw.base == std::string::npos) ? 0 : (int)(pos + 1);
    info.ftw.level = 0;
    queue.push_back(info);

    while (!queue.empty()) {
        info = queue.front();
        queue.pop_front();

        c = info.path[info.path.length() - 1];
        info2.path = info.path;
        pos = info2.path.length() + 1;
        info2.ftw.base = (int)pos;
        info2.ftw.level = info.ftw.level + 1;
        if ((c != _FS_BSLASH) && (c != _FS_COLON)) {
            info2.path.append(1, _FS_BSLASH);
        }
        info2.path.append(1, _FS_STAR);

        hFind = ::FindFirstFileA(info2.path.c_str(), &ffd);
        if (INVALID_HANDLE_VALUE == hFind) {
            return false;
        }

        do {
            if ((ffd.cFileName[0] == _FS_NULL) || (::strcmp(ffd.cFileName, ".") == 0) ||
                (::strcmp(ffd.cFileName, "..") == 0)) {
                continue;
            }

            info2.path.replace(pos, std::string::npos, ffd.cFileName);

            if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
                if (recursive) {
                    queue.push_front(info2);
                } else {
                    queue2.push_front(info2);
                }
            } else {
                ret = handler(info2.path.c_str(), FTW_F, &info2.ftw);
                if (ret != FTW_CONTINUE) {
                    ::FindClose(hFind);
                    return false;
                }
            }
        } while (::FindNextFileA(hFind, &ffd) != 0);

        dwError = ::GetLastError();
        ::FindClose(hFind);
        if (dwError != ERROR_NO_MORE_FILES) {
            return false;
        }

        queue2.push_front(info);
    }

    for (auto &info3 : queue2) {
        ret = handler(info3.path.c_str(), FTW_DP, &info3.ftw);
        if (ret != FTW_CONTINUE) {
            return false;
        }
    }

    return true;
#else
    tls_ftw_ctx.handler = &handler;
    tls_ftw_ctx.recursive = recursive;
    int flags =
#ifdef __linux__
        FTW_ACTIONRETVAL
#else
        0
#endif
        ;
    if (recursive) {
        flags |= FTW_DEPTH;
    }
    int ret = ::nftw(dirpath.c_str(), ftw_wrapper, 1, flags);

    return (ret == 0);
#endif
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
    return dsn::utils::filesystem::file_tree_walk(
        npath,
        [](const char *fpath, int typeflag, struct FTW *ftwbuf) {
            bool succ;

            dassert(
                (typeflag == FTW_F) || (typeflag == FTW_DP), "Invalid typeflag = %d.", typeflag);
#ifdef _WIN32
            if (typeflag != FTW_F) {
                succ = (::RemoveDirectoryA(fpath) == TRUE);
                if (!succ) {
                    dwarn("remove directory %s failed, err = %d", fpath, ::GetLastError());
                }
            } else {
#endif
                succ = (::remove(fpath) == 0);
                if (!succ) {
                    dwarn("remove file %s failed, err = %s", fpath, strerror(errno));
                }
#ifdef _WIN32
            }
#endif

            return (succ ? FTW_CONTINUE : FTW_STOP);
        },
        true);
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
            dwarn("remove file %s failed, err = %s", path.c_str(), strerror(errno));
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

//
// on linux, we don't need to check this existence of path2 since ::rename() will do this.
// however, rename will not do this on windows as on linux, so we do this here for windows
//
#if defined(_WIN32)
    if (dsn::utils::filesystem::path_exists(path2)) {
        ret = dsn::utils::filesystem::remove_path(path2);
        if (!ret) {
            dwarn("rename from '%s' to '%s' failed to remove the existed destinate path, err = %s",
                  path1.c_str(),
                  path2.c_str(),
                  strerror(errno));

            return ret;
        }
    }
#endif

    ret = (::rename(path1.c_str(), path2.c_str()) == 0);
    if (!ret) {
        dwarn("rename from '%s' to '%s' failed, err = %s",
              path1.c_str(),
              path2.c_str(),
              strerror(errno));
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
#ifdef _WIN32
    sep = _FS_BSLASH;
    if (npath.compare(0, 4, "\\\\?\\") == 0) {
        prev = 4;
    } else if (npath.compare(0, 2, "\\\\") == 0) {
        prev = 2;
    } else if (npath.compare(1, 2, ":\\") == 0) {
        prev = 3;
    }
#else
    sep = _FS_SLASH;
    if (npath[0] == sep) {
        prev = 1;
    }
#endif

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
    dwarn("create_directory %s failed due to cannot create the component: %s, err = %s",
          path.c_str(),
          cpath.c_str(),
          strerror(err));
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

#ifdef _WIN32
    mode = _S_IREAD | _S_IWRITE;
    if (::_sopen_s(&fd, npath.c_str(), _O_WRONLY | _O_CREAT | _O_TRUNC, _SH_DENYRW, mode) != 0)
#else
    mode = 0775;
    fd = ::creat(npath.c_str(), mode);
    if (fd == -1)
#endif
    {
        err = errno;
        dwarn("create_file %s failed, err = %s", path.c_str(), strerror(err));
        return false;
    }

    if (::close_(fd) != 0) {
        dwarn("create_file %s, failed to close the file handle.", path.c_str());
    }

    return true;
}

bool get_absolute_path(const std::string &path1, std::string &path2)
{
    bool succ;
#if defined(_WIN32)
    char *component;
    succ =
        (0 != ::GetFullPathNameA(path1.c_str(), TLS_PATH_BUFFER_SIZE, tls_path_buffer, &component));
#else
    succ = (::realpath(path1.c_str(), tls_path_buffer) != nullptr);
#endif
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
#ifdef _WIN32
        pos = path.find_last_of(_FS_COLON);
        if (pos == last) {
            return "";
        }
#else
        return path;
#endif
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
#ifdef _WIN32
        if (path1[path1.length() - 1] != _FS_COLON) {
#endif
            path3.append(1,
#ifdef _WIN32
                         _FS_BSLASH
#else
                     _FS_SLASH
#endif
                         );
#ifdef _WIN32
        }
#endif
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

#if defined(__linux__) || defined(__APPLE__)
    int err;
#endif

#if defined(_WIN32)
    HANDLE hProcess;
    DWORD dwSize = TLS_PATH_BUFFER_SIZE;

    if (pid == -1) {
        hProcess = ::GetCurrentProcess();
    } else {
        hProcess = ::OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, (DWORD)pid);
        if (hProcess == nullptr) {
            return ERR_INVALID_HANDLE;
        }
    }

    if (::QueryFullProcessImageNameA(hProcess, 0, tls_path_buffer, &dwSize) == FALSE) {
        return ERR_PATH_NOT_FOUND;
    }

    path = tls_path_buffer;
#elif defined(__linux__)
    char tmp[48];

    err = snprintf_p(
        tmp, ARRAYSIZE(tmp), "/proc/%s/exe", (pid == -1) ? "self" : std::to_string(pid).c_str());
    dassert(err >= 0, "snprintf_p failed.");

    err = (int)readlink(tmp, tls_path_buffer, TLS_PATH_BUFFER_SIZE);
    if (err == -1) {
        return ERR_PATH_NOT_FOUND;
    }

    tls_path_buffer[err] = 0;
    path = tls_path_buffer;
#elif defined(__FreeBSD__)
    struct kinfo_proc *proc;

    if (pid == -1) {
        pid = (int)getpid();
    }

    proc = kinfo_getproc(pid);
    if (proc == nullptr) {
        return ERR_PATH_NOT_FOUND;
    }

    // proc->ki_comm is the command name instead of the full name.
    if (!dsn::utils::filesystem::get_absolute_path(proc->ki_comm, path)) {
        return ERR_PATH_NOT_FOUND;
    }
    free(proc);
#elif defined(__APPLE__)
    if (pid == -1) {
        pid = getpid();
    }

    err = proc_pidpath(pid, tls_path_buffer, TLS_PATH_BUFFER_SIZE);
    if (err <= 0) {
        return ERR_PATH_NOT_FOUND;
    }

    path = tls_path_buffer;
#else
#error not implemented yet
#endif

    return ERR_OK;
}

bool get_disk_space_info(const std::string &path, disk_space_info &info)
{

    boost::system::error_code ec;
    boost::filesystem::space_info in = boost::filesystem::space(path, ec);
    if (ec) {
        derror(
            "get disk space info failed: path = %s, err = %s", path.c_str(), ec.message().c_str());
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
#if defined(_WIN32)
#error not implemented yet
#else
    err = ::link(src.c_str(), target.c_str());
#endif
    return (err == 0);
}

error_code md5sum(const std::string &file_path, /*out*/ std::string &result)
{
    result.clear();
    // if file not exist, we return ERR_OBJECT_NOT_FOUND
    if (!::dsn::utils::filesystem::file_exists(file_path)) {
        derror("md5sum error: file %s not exist", file_path.c_str());
        return ERR_OBJECT_NOT_FOUND;
    }

#if defined(_WIN32)
#error not implemented yet
#else
    FILE *fp = fopen(file_path.c_str(), "rb");
    if (fp == nullptr) {
        derror("md5sum error: open file %s failed", file_path.c_str());
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
                derror("md5sum error: read file %s failed: errno = %d (%s)",
                       file_path.c_str(), err, strerror(err));
                fclose(fp);
                MD5_Final(out, &c);
                return ERR_FILE_OPERATION_FAILED;
            }
        }
    }
    fclose(fp);
    MD5_Final(out, &c);

    char str[MD5_DIGEST_LENGTH * 2 + 1];
    for(int n = 0; n < MD5_DIGEST_LENGTH; n++)
        sprintf(str + n + n, "%02x", out[n]);
    result.assign(str);
#endif

    return ERR_OK;
}

std::pair<error_code, bool> is_directory_empty(const std::string &dirname)
{
    std::pair<error_code, bool> res;
    res.first = ERR_OK;
#if defined(_WIN32)
#error not implemented yet
#else
    std::vector<std::string> subfiles;
    std::vector<std::string> subdirs;
    if (get_subfiles(dirname, subfiles, false) && get_subdirectories(dirname, subdirs, false)) {
        res.second = subfiles.empty() && subdirs.empty();
    } else {
        res.first = ERR_FILE_OPERATION_FAILED;
    }
#endif
    return res;
}
}
}
}
