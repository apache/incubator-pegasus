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
 *     Unit-test for hpc tail logger.
 *
 * Revision history:
 *     Nov., 2015, @xiaotz (Xiaotong Zhang), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <sstream>
#include <vector>
#include <string>
#include <queue>
#include <iostream>
#include <dsn/tool-api/aio_provider.h>
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/utility/priority_queue.h>
#include <dsn/tool-api/command_manager.h>

#include "../core/group_address.h"
#include "test_utils.h"
#include "../core/service_engine.h"
#include "../tools/hpc/hpc_tail_logger.h"

TEST(tools_hpc, tail_logger)
{
    dwarn("this is a warning");
    dinfo("this is a log %d", 1);
}

TEST(tools_hpc, tail_logger_cb)
{
    std::string output;
    bool ret = dsn::command_manager::instance().run_command("tail-log", output);
    if (!ret)
        return;

    EXPECT_TRUE(strcmp(output.c_str(), "invalid arguments for tail-log command") == 0);

    std::ostringstream in;
    in << "tail-log 12345 4 1 " << dsn::utils::get_current_tid();
    std::this_thread::sleep_for(std::chrono::seconds(3));
    dwarn("key:12345");
    std::this_thread::sleep_for(std::chrono::seconds(2));
    dwarn("key:12345");
    output.clear();
    dsn::command_manager::instance().run_command(in.str().c_str(), output);
    EXPECT_TRUE(strstr(output.c_str(), "In total (1) log entries are found between") != nullptr);
    dsn::command_manager::instance().run_command("tail-log-dump", output);

    ::dsn::logging_provider *logger = ::dsn::service_engine::fast_instance().logging();
    if (logger != nullptr) {
        logger->flush();
    }
}
