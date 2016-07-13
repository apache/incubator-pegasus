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
 *     a singleton to manager all zookeeper sessions, so that each zookeeper session
 *     can be shared by all threads in one service-node. The Header file.
 *
 * Revision history:
 *     2015-12-04, @shengofsun (sunweijie@xiaomi.com)
 */

#include <dsn/utility/singleton_store.h>
#include <string>

#pragma once

namespace dsn { namespace dist{

class zookeeper_session;
class zookeeper_session_mgr: public utils::singleton<zookeeper_session_mgr>
{
public:
    zookeeper_session_mgr();
    zookeeper_session* get_session(dsn_app_info* node);
    const char* zoo_hosts() const { return _zoo_hosts.c_str(); }
    int timeout() const { return _timeout_ms; }
    const char* zoo_logfile() const { return _zoo_logfile.c_str(); }

private:
    utils::ex_lock_nr _store_lock;

private:
    std::string _zoo_hosts;
    int _timeout_ms;
    std::string _zoo_logfile;
};

}}
