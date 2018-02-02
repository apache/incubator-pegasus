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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/cpp/auto_codes.h>
#include <dsn/cpp/callocator.h>
#include <functional>

#ifdef DSN_USE_THRIFT_SERIALIZATION
#include <thrift/protocol/TProtocol.h>
#endif

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "utils"

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
template <typename T>
std::shared_ptr<T> make_shared_array(size_t size)
{
    return std::shared_ptr<T>(new T[size], std::default_delete<T[]>());
}

class blob
{
public:
    blob() : _buffer(nullptr), _data(nullptr), _length(0) {}

    blob(const std::shared_ptr<char> &buffer, unsigned int length)
        : _holder(buffer), _buffer(_holder.get()), _data(_holder.get()), _length(length)
    {
    }

    blob(std::shared_ptr<char> &&buffer, unsigned int length)
        : _holder(std::move(buffer)), _buffer(_holder.get()), _data(_holder.get()), _length(length)
    {
    }

    blob(const std::shared_ptr<char> &buffer, int offset, unsigned int length)
        : _holder(buffer), _buffer(_holder.get()), _data(_holder.get() + offset), _length(length)
    {
    }

    blob(std::shared_ptr<char> &&buffer, int offset, unsigned int length)
        : _holder(std::move(buffer)),
          _buffer(_holder.get()),
          _data(_holder.get() + offset),
          _length(length)
    {
    }

    blob(const char *buffer, int offset, unsigned int length)
        : _buffer(buffer), _data(buffer + offset), _length(length)
    {
    }

    blob(const blob &source)
        : _holder(source._holder),
          _buffer(source._buffer),
          _data(source._data),
          _length(source._length)
    {
    }

    blob(blob &&source)
        : _holder(std::move(source._holder)),
          _buffer(source._buffer),
          _data(source._data),
          _length(source._length)
    {
        source._buffer = nullptr;
        source._data = nullptr;
        source._length = 0;
    }

    blob &operator=(const blob &that)
    {
        _holder = that._holder;
        _buffer = that._buffer;
        _data = that._data;
        _length = that._length;
        return *this;
    }

    blob &operator=(blob &&that)
    {
        _holder = std::move(that._holder);
        _buffer = that._buffer;
        _data = that._data;
        _length = that._length;
        that._buffer = nullptr;
        that._data = nullptr;
        that._length = 0;
        return *this;
    }

    void assign(const std::shared_ptr<char> &buffer, int offset, unsigned int length)
    {
        _holder = buffer;
        _buffer = _holder.get();
        _data = _holder.get() + offset;
        _length = length;
    }

    void assign(std::shared_ptr<char> &&buffer, int offset, unsigned int length)
    {
        _holder = std::move(buffer);
        _buffer = (_holder.get());
        _data = (_holder.get() + offset);
        _length = length;
    }

    void assign(const char *buffer, int offset, unsigned int length)
    {
        _holder = nullptr;
        _buffer = buffer;
        _data = buffer + offset;
        _length = length;
    }

    const char *data() const { return _data; }

    unsigned int length() const { return _length; }

    std::shared_ptr<char> buffer() const { return _holder; }

    bool has_holder() const { return _holder.get() != nullptr; }

    const char *buffer_ptr() const { return _holder.get(); }

    // offset can be negative for buffer dereference
    blob range(int offset) const
    {
        dassert(offset <= static_cast<int>(_length),
                "offset cannot exceed the current length value");

        blob temp = *this;
        temp._data += offset;
        temp._length -= offset;
        return temp;
    }

    blob range(int offset, unsigned int len) const
    {
        dassert(offset <= static_cast<int>(_length),
                "offset cannot exceed the current length value");

        blob temp = *this;
        temp._data += offset;
        temp._length -= offset;
        dassert(temp._length >= len, "buffer length must exceed the required length");
        temp._length = len;
        return temp;
    }

    bool operator==(const blob &r) const
    {
        dassert(false, "not implemented");
        return false;
    }

#ifdef DSN_USE_THRIFT_SERIALIZATION
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;
#endif
private:
    friend class binary_writer;
    std::shared_ptr<char> _holder;
    const char *_buffer;
    const char *_data;
    unsigned int _length; // data length
};

class binary_reader
{
public:
    // given bb on ctor
    binary_reader(const blob &blob);

    // or delayed init
    binary_reader() {}

    virtual ~binary_reader() {}

    void init(const blob &bb);

    template <typename T>
    int read_pod(/*out*/ T &val);
    template <typename T>
    int read(/*out*/ T &val)
    {
        dassert(false, "read of this type is not implemented");
        return 0;
    }
    int read(/*out*/ int8_t &val) { return read_pod(val); }
    int read(/*out*/ uint8_t &val) { return read_pod(val); }
    int read(/*out*/ int16_t &val) { return read_pod(val); }
    int read(/*out*/ uint16_t &val) { return read_pod(val); }
    int read(/*out*/ int32_t &val) { return read_pod(val); }
    int read(/*out*/ uint32_t &val) { return read_pod(val); }
    int read(/*out*/ int64_t &val) { return read_pod(val); }
    int read(/*out*/ uint64_t &val) { return read_pod(val); }
    int read(/*out*/ bool &val) { return read_pod(val); }

    int read(/*out*/ std::string &s);
    int read(char *buffer, int sz);
    int read(blob &blob);

    bool next(const void **data, int *size);
    bool skip(int count);
    bool backup(int count);

    blob get_buffer() const { return _blob; }
    blob get_remaining_buffer() const { return _blob.range(static_cast<int>(_ptr - _blob.data())); }
    bool is_eof() const { return _ptr >= _blob.data() + _size; }
    int total_size() const { return _size; }
    int get_remaining_size() const { return _remaining_size; }

private:
    blob _blob;
    int _size;
    const char *_ptr;
    int _remaining_size;
};

class binary_writer
{
public:
    binary_writer(int reserved_buffer_size = 0);
    binary_writer(blob &buffer);
    virtual ~binary_writer();

    virtual void flush();

    template <typename T>
    void write_pod(const T &val);
    template <typename T>
    void write(const T &val)
    {
        dassert(false, "write of this type is not implemented");
    }
    void write(const int8_t &val) { write_pod(val); }
    void write(const uint8_t &val) { write_pod(val); }
    void write(const int16_t &val) { write_pod(val); }
    void write(const uint16_t &val) { write_pod(val); }
    void write(const int32_t &val) { write_pod(val); }
    void write(const uint32_t &val) { write_pod(val); }
    void write(const int64_t &val) { write_pod(val); }
    void write(const uint64_t &val) { write_pod(val); }
    void write(const bool &val) { write_pod(val); }

    void write(const std::string &val);
    void write(const char *buffer, int sz);
    void write(const blob &val);
    void write_empty(int sz);

    bool next(void **data, int *size);
    bool backup(int count);

    void get_buffers(/*out*/ std::vector<blob> &buffers);
    int get_buffer_count() const { return static_cast<int>(_buffers.size()); }
    blob get_buffer();
    blob get_current_buffer(); // without commit, write can be continued on the last buffer
    blob get_first_buffer() const;

    int total_size() const { return _total_size; }

protected:
    // bb may have large space than size
    void create_buffer(size_t size);
    void commit();
    virtual void create_new_buffer(size_t size, /*out*/ blob &bb);

private:
    std::vector<blob> _buffers;

    char *_current_buffer;
    int _current_offset;
    int _current_buffer_length;

    int _total_size;
    int _reserved_size_per_buffer;
    static int _reserved_size_per_buffer_static;
};

//--------------- inline implementation -------------------
template <typename T>
inline int binary_reader::read_pod(/*out*/ T &val)
{
    if (sizeof(T) <= get_remaining_size()) {
        memcpy((void *)&val, _ptr, sizeof(T));
        _ptr += sizeof(T);
        _remaining_size -= sizeof(T);
        return static_cast<int>(sizeof(T));
    } else {
        dassert(false, "read beyond the end of buffer");
        return 0;
    }
}

template <typename T>
inline void binary_writer::write_pod(const T &val)
{
    write((char *)&val, static_cast<int>(sizeof(T)));
}

inline void binary_writer::get_buffers(/*out*/ std::vector<blob> &buffers)
{
    commit();
    buffers = _buffers;
}

inline blob binary_writer::get_first_buffer() const { return _buffers[0]; }

inline void binary_writer::write(const std::string &val)
{
    int len = static_cast<int>(val.length());
    write((const char *)&len, sizeof(int));
    if (len > 0)
        write((const char *)&val[0], len);
}

inline void binary_writer::write(const blob &val)
{
    // TODO: optimization by not memcpy
    int len = val.length();
    write((const char *)&len, sizeof(int));
    if (len > 0)
        write((const char *)val.data(), len);
}
}

namespace dsn {
namespace utils {

extern void
split_args(const char *args, /*out*/ std::vector<std::string> &sargs, char splitter = ' ');
extern void
split_args(const char *args, /*out*/ std::list<std::string> &sargs, char splitter = ' ');
extern std::string
replace_string(std::string subject, const std::string &search, const std::string &replace);
extern std::string get_last_component(const std::string &input, const char splitters[]);

extern char *trim_string(char *s);

extern uint64_t get_current_physical_time_ns();

inline uint64_t get_current_rdtsc()
{
#ifdef _WIN32
    return __rdtsc();
#else
    unsigned hi, lo;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
#endif
}

extern void time_ms_to_string(uint64_t ts_ms, char *str);             // yyyy-MM-dd hh:mm:ss.SSS
extern void time_ms_to_date(uint64_t ts_ms, char *str, int len);      // yyyy-MM-dd
extern void time_ms_to_date_time(uint64_t ts_ms, char *str, int len); // yyyy-MM-dd hh:mm:ss
extern void time_ms_to_date_time(uint64_t ts_ms,
                                 int32_t &hour,
                                 int32_t &min,
                                 int32_t &sec); // time to hour, min, sec

extern int get_current_tid_internal();

typedef struct _tls_tid
{
    unsigned int magic;
    int local_tid;
} tls_tid;
extern __thread tls_tid s_tid;

inline int get_current_tid()
{
    if (s_tid.magic == 0xdeadbeef)
        return s_tid.local_tid;
    else {
        s_tid.magic = 0xdeadbeef;
        s_tid.local_tid = get_current_tid_internal();
        return s_tid.local_tid;
    }
}

inline int get_invalid_tid() { return -1; }

namespace filesystem {

extern bool get_absolute_path(const std::string &path1, std::string &path2);

extern std::string remove_file_name(const std::string &path);

extern std::string get_file_name(const std::string &path);

extern std::string path_combine(const std::string &path1, const std::string &path2);

extern int get_normalized_path(const std::string &path, std::string &npath);

// int (const char* fpath, int typeflags, struct FTW *ftwbuf)
typedef std::function<int(const char *, int, struct FTW *)> ftw_handler;

extern bool file_tree_walk(const std::string &dirpath, ftw_handler handler, bool recursive = true);

extern bool path_exists(const std::string &path);

extern bool directory_exists(const std::string &path);

extern bool file_exists(const std::string &path);

extern bool
get_subfiles(const std::string &path, std::vector<std::string> &sub_list, bool recursive);

extern bool
get_subdirectories(const std::string &path, std::vector<std::string> &sub_list, bool recursive);

extern bool
get_subpaths(const std::string &path, std::vector<std::string> &sub_list, bool recursive);

extern bool remove_path(const std::string &path);

// this will always remove target path if exist
extern bool rename_path(const std::string &path1, const std::string &path2);

extern bool file_size(const std::string &path, int64_t &sz);

extern bool create_directory(const std::string &path);

extern bool create_file(const std::string &path);

extern bool get_current_directory(std::string &path);

extern bool last_write_time(const std::string &path, time_t &tm);

extern error_code get_process_image_path(int pid, std::string &path);

inline error_code get_current_process_image_path(std::string &path)
{
    auto err = dsn::utils::filesystem::get_process_image_path(-1, path);
    dassert(err == ERR_OK, "get_current_process_image_path failed.");
    return err;
}

struct disk_space_info
{
    // all values are byte counts
    uint64_t capacity;
    uint64_t available;
};
extern bool get_disk_space_info(const std::string &path, disk_space_info &info);

extern bool link_file(const std::string &src, const std::string &target);

extern error_code md5sum(const std::string &file_path, /*out*/ std::string &result);

// return value:
//  - <A, B>:
//          A is represent whether operation encounter some local error
//          B is represent wheter the directory is empty, true means empty, otherwise false
extern std::pair<error_code, bool> is_directory_empty(const std::string &dirname);
}
}
} // end namespace dsn::utils
