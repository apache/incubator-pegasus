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

#include "zookeeper_session_mgr.h"

#include <stdio.h>
#include <zookeeper/zookeeper.h>
#include <functional>

#include "runtime/service_app.h"
#include "utils/flags.h"
#include "utils/singleton_store.h"
#include "zookeeper_session.h"

namespace dsn {
namespace dist {

DSN_DEFINE_string(zookeeper, logfile, "", "The Zookeeper logfile");

zookeeper_session_mgr::zookeeper_session_mgr()
{
    FILE *fp = fopen(FLAGS_logfile, "a");
    if (fp != nullptr)
        zoo_set_log_stream(fp);
}

zookeeper_session *zookeeper_session_mgr::get_session(const service_app_info &info)
{
    auto &store = utils::singleton_store<int, zookeeper_session *>::instance();
    zookeeper_session *ans = nullptr;
    if (!store.get(info.entity_id, ans)) {
        ans = new zookeeper_session(info);
        store.put(info.entity_id, ans);
    }
    return ans;
}
}
}
