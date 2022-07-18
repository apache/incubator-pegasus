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

#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <stdlib.h> // posix_memalign
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h> // getpagesize

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/flags.h>

#include "block_service/directio_writable_file.h"

namespace dsn {
namespace dist {
namespace block_service {

DSN_DEFINE_uint32("replication",
                  direct_io_buffer_pages,
                  64,
                  "Number of pages we need to set to direct io buffer");
DSN_TAG_VARIABLE(direct_io_buffer_pages, FT_MUTABLE);

DSN_DEFINE_bool("replication",
                enable_direct_io,
                false,
                "Whether to enable direct I/O when download files");
DSN_TAG_VARIABLE(enable_direct_io, FT_MUTABLE);

const uint32_t g_page_size = getpagesize();

direct_io_writable_file::direct_io_writable_file(const std::string &file_path)
    : _file_path(file_path),
      _fd(-1),
      _file_size(0),
      _buffer(nullptr),
      _buffer_size(FLAGS_direct_io_buffer_pages * g_page_size),
      _offset(0)
{
}

direct_io_writable_file::~direct_io_writable_file()
{
    if (!_buffer || _fd < 0) {
        return;
    }
    // Here is an ensurance, users shuold call finalize manually
    dassert(_offset == 0, "finalize() should be called before destructor");

    free(_buffer);
    close(_fd);
}

bool direct_io_writable_file::initialize()
{
    if (posix_memalign(&_buffer, g_page_size, _buffer_size) != 0) {
        derror_f("Allocate memaligned buffer failed, errno = {}", errno);
        return false;
    }

    int flag = O_WRONLY | O_TRUNC | O_CREAT;
#if !defined(__APPLE__)
    flag |= O_DIRECT;
#endif
    _fd = open(_file_path.c_str(), flag, S_IRUSR | S_IWUSR | S_IRGRP);
    if (_fd < 0) {
        derror_f("Failed to open {} with flag {}, errno = {}", _file_path, flag, errno);
        free(_buffer);
        _buffer = nullptr;
        return false;
    }
    return true;
}

bool direct_io_writable_file::finalize()
{
    dassert(_buffer && _fd >= 0, "Initialize the instance first");

    if (_offset > 0) {
        if (::write(_fd, _buffer, _buffer_size) != _buffer_size) {
            derror_f("Failed to write last chunk, filie_path = {}, errno = {}", _file_path, errno);
            return false;
        }
        _offset = 0;
        ftruncate(_fd, _file_size);
    }
    return true;
}

bool direct_io_writable_file::write(const char *s, size_t n)
{
    dassert(_buffer && _fd >= 0, "Initialize the instance first");

    uint32_t remaining = n;
    while (remaining > 0) {
        uint32_t bytes = std::min((_buffer_size - _offset), remaining);
        memcpy((char *)_buffer + _offset, s, bytes);
        _offset += bytes;
        remaining -= bytes;
        s += bytes;
        // buffer is full, flush to file
        if (_offset == _buffer_size) {
            if (::write(_fd, _buffer, _buffer_size) != _buffer_size) {
                derror_f("Failed to write to direct_io_writable_file, errno = {}", errno);
                return false;
            }
            // reset offset
            _offset = 0;
        }
    }
    _file_size += n;
    return true;
}

} // namespace block_service
} // namespace dist
} // namespace dsn
