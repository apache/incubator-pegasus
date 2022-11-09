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

#include "utils/safe_strerror_posix.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/fmt_logging.h"
#include "utils/rpc_address.h"
#include "utils/crc.h"
#include <cstdio>
#include <cerrno>
#include <iostream>

#define log_error_and_return(buffer, length)                                                       \
    do {                                                                                           \
        ::dsn::utils::safe_strerror_r(errno, buffer, length);                                      \
        LOG_ERROR("append file failed, reason(%s)", buffer);                                       \
        return -1;                                                                                 \
    } while (0)

struct block_header
{
    uint32_t length;
    uint32_t crc32;
};

class dump_file
{
public:
    ~dump_file() { fclose(_file_handle); }

    static std::shared_ptr<dump_file> open_file(const char *filename, bool is_write)
    {
        std::shared_ptr<dump_file> res(new dump_file());
        res->_filename = filename;
        if (is_write)
            res->_file_handle = fopen(filename, "wb");
        else
            res->_file_handle = fopen(filename, "rb");
        res->_is_write = is_write;

        if (res->_file_handle == nullptr)
            return nullptr;
        return res;
    }

    int append_buffer(const char *data, uint32_t data_length)
    {
        static __thread char msg_buffer[128];

        CHECK(_is_write, "call append when open file with read mode");

        block_header hdr = {data_length, 0};
        hdr.crc32 = dsn::utils::crc32_calc(data, data_length, _crc);
        _crc = hdr.crc32;
        size_t len = fwrite(&hdr, sizeof(hdr), 1, _file_handle);
        if (len < 1) {
            log_error_and_return(msg_buffer, 128);
        }

        len = 0;
        while (len < data_length) {
            size_t cnt = fwrite(data + len, 1, data_length - len, _file_handle);
            if (len + cnt < data_length && errno != EINTR) {
                log_error_and_return(msg_buffer, 128);
            }
            len += cnt;
        }
        return 0;
    }
    int append_buffer(const dsn::blob &data) { return append_buffer(data.data(), data.length()); }
    int append_buffer(const std::string &data) { return append_buffer(data.c_str(), data.size()); }
    int read_next_buffer(/*out*/ dsn::blob &output)
    {
        static __thread char msg_buffer[128];
        CHECK(!_is_write, "call read next buffer when open file with write mode");

        block_header hdr;
        size_t len = fread(&hdr, sizeof(hdr), 1, _file_handle);
        if (len < 1) {
            if (feof(_file_handle))
                return 0;
            else {
                log_error_and_return(msg_buffer, 128);
            }
        }

        std::shared_ptr<char> ptr(dsn::utils::make_shared_array<char>(hdr.length));
        char *raw_mem = ptr.get();
        len = 0;
        while (len < hdr.length) {
            size_t cnt = fread(raw_mem + len, 1, hdr.length - len, _file_handle);
            if (len + cnt < hdr.length) {
                if (feof(_file_handle)) {
                    LOG_ERROR("unexpected file end, start offset of this block (%u)",
                              ftell(_file_handle) - len - sizeof(hdr));
                    return -1;
                } else if (errno != EINTR) {
                    log_error_and_return(msg_buffer, 128);
                }
            }
            len += cnt;
        }
        _crc = dsn::utils::crc32_calc(raw_mem, len, _crc);
        if (_crc != hdr.crc32) {
            LOG_ERROR("file %s data error, block offset(%ld)",
                      _filename.c_str(),
                      ftell(_file_handle) - hdr.length - sizeof(hdr));
            return -1;
        }

        output.assign(ptr, 0, hdr.length);
        return 1;
    }

private:
    dump_file() : _file_handle(nullptr), _crc(0) {}
    bool _is_write; // true for write, false for read
    FILE *_file_handle;
    std::string _filename;
    uint32_t _crc;
};
