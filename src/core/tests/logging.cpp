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
 *     Unit-test for logging.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/service_api_c.h>
#include <gtest/gtest.h>
#include <iostream>

TEST(core, logging)
{
    dsn_log_level_t level = dsn_log_get_start_level();
    std::cout << "logging start level = " << level << std::endl;
    dsn_logf(__FILENAME__,
             __FUNCTION__,
             __LINE__,
             dsn_log_level_t::LOG_LEVEL_DEBUG,
             "in TEST(core, logging)");
    dsn_log(__FILENAME__, __FUNCTION__, __LINE__, dsn_log_level_t::LOG_LEVEL_DEBUG);
}

TEST(core, logging_big_log)
{
    std::string big_str(128000, 'x');
    dsn_logf(__FILENAME__,
             __FUNCTION__,
             __LINE__,
             dsn_log_level_t::LOG_LEVEL_DEBUG,
             "write big str %s",
             big_str.c_str());
}
