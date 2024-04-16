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

#include <stdint.h>
#include <memory>
#include <string>

#include "utils/error_code.h"
#include "utils/factory_store.h"

namespace rocksdb {
class RandomAccessFile;
class RandomRWFile;
} // namespace rocksdb

namespace dsn {

class aio_context;
class aio_task;
class disk_engine;

#define DSN_INVALID_FILE_HANDLE -1
struct linux_fd_t
{
    int fd;

    explicit linux_fd_t(int f) : fd(f) {}
};

class aio_provider
{
public:
    template <typename T>
    static aio_provider *create(disk_engine *disk)
    {
        return new T(disk);
    }

    typedef aio_provider *(*factory)(disk_engine *);

    explicit aio_provider(disk_engine *disk);
    virtual ~aio_provider() = default;

    virtual std::unique_ptr<rocksdb::RandomAccessFile> open_read_file(const std::string &fname) = 0;
    virtual error_code read(const aio_context &aio_ctx, /*out*/ uint64_t *processed_bytes) = 0;

    virtual std::unique_ptr<rocksdb::RandomRWFile> open_write_file(const std::string &fname) = 0;
    virtual error_code write(const aio_context &aio_ctx, /*out*/ uint64_t *processed_bytes) = 0;
    virtual error_code flush(rocksdb::RandomRWFile *rwf) = 0;
    virtual error_code close(rocksdb::RandomRWFile *rwf) = 0;

    // Submits the aio_task to the underlying disk-io executor.
    // This task may not be executed immediately, call `aio_task::wait`
    // to wait until it completes.
    virtual void submit_aio_task(aio_task *aio) = 0;

    virtual aio_context *prepare_aio_context(aio_task *) = 0;

    void complete_io(aio_task *aio, error_code err, uint64_t bytes);

private:
    disk_engine *_engine;
};

namespace tools {
namespace internal_use_only {
bool register_component_provider(const char *name,
                                 aio_provider::factory f,
                                 dsn::provider_type type);
} // namespace internal_use_only
} // namespace tools

} // namespace dsn
