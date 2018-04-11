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
 *     Disk IO performance test
 *
 * Revision history:
 *     2016-01-05, Tianyi Wang, first version
 */

#include <cinttypes>
#include <dsn/tool-api/rpc_address.h>
#include <dsn/tool-api/aio_provider.h>
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/utility/priority_queue.h>
#include <dsn/utility/filesystem.h>
#include <dsn/tool-api/group_address.h>
#include "test_utils.h"
#include <boost/lexical_cast.hpp>

DEFINE_TASK_CODE_AIO(LPC_AIO_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)
void aio_testcase(uint64_t block_size, size_t concurrency, bool is_write, bool random_offset)
{
    std::chrono::steady_clock clock;
    std::unique_ptr<char[]> buffer(new char[block_size]);
    std::atomic_uint remain_concurrency;
    remain_concurrency = concurrency;
    if (is_write && utils::filesystem::file_exists("temp")) {
        utils::filesystem::remove_path("temp");
        dassert(!utils::filesystem::file_exists("temp"), "");
    }
    auto file_handle = dsn_file_open("temp", O_CREAT | O_RDWR, 0666);
    auto total_size_mb = 64;
    auto total_size_bytes = total_size_mb * 1024 * 1024;
    auto tic = clock.now();

    for (int bytes_written = 0; bytes_written < total_size_bytes;) {
        while (true) {
            if (remain_concurrency.fetch_sub(1, std::memory_order_acquire) <= 0) {
                remain_concurrency.fetch_add(1, std::memory_order_relaxed);
            } else {
                break;
            }
        }
        auto cb = [&](error_code ec, int sz) {
            dassert(ec == ERR_OK && uint64_t(sz) == block_size,
                    "ec = %s, sz = %d, block_size = %" PRId64 "",
                    ec.to_string(),
                    sz,
                    block_size);
            remain_concurrency.fetch_add(1, std::memory_order_relaxed);
        };
        auto offset =
            random_offset ? dsn_random64(0, total_size_bytes - block_size) : bytes_written;
        if (is_write) {
            file::write(file_handle, buffer.get(), block_size, offset, LPC_AIO_TEST, nullptr, cb);
        } else {
            file::read(file_handle, buffer.get(), block_size, offset, LPC_AIO_TEST, nullptr, cb);
        }

        bytes_written += block_size;
    }
    while (remain_concurrency != concurrency) {
        ;
    }
    dsn_file_flush(file_handle);
    auto toc = clock.now();
    dsn_file_close(file_handle);
    std::cout << "is_write = " << is_write << " random_offset = " << random_offset
              << " block_size = " << block_size << " concurrency = " << concurrency
              << " throughput = "
              << double(total_size_mb) * 1000000 /
                     std::chrono::duration_cast<std::chrono::microseconds>(toc - tic).count()
              << " mB/s" << std::endl;
}
TEST(core, aio_perf_test)
{
    auto block_size_list = {64, 512, 4096, 512 * 1024};
    auto concurrency_list = {1, 4, 16};
    for (auto is_write : {true, false}) {
        for (auto random_offset : {true, false}) {
            for (auto block_size : block_size_list) {
                for (auto concurrency : concurrency_list) {
                    aio_testcase(block_size, concurrency, is_write, random_offset);
                }
            }
        }
    }
}
