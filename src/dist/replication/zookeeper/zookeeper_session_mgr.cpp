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
 *     can be shared by all threads in one service-node. The implementation file.
 *
 * Revision history:
 *     2015-12-04, @shengofsun (sunweijie@xiaomi.com)
 */
#include "zookeeper_session_mgr.h"
#include "zookeeper_session.h"

#include <stdio.h>
#include <zookeeper/zookeeper.h>
#include <stdexcept>

namespace dsn {
namespace dist {

zookeeper_session_mgr::zookeeper_session_mgr()
{
    _zoo_hosts = dsn_config_get_value_string("zookeeper", "hosts_list", "", "zookeeper_hosts");
    _timeout_ms = dsn_config_get_value_uint64(
        "zookeeper", "timeout_ms", 30000, "zookeeper_timeout_milliseconds");
    _zoo_logfile = dsn_config_get_value_string("zookeeper", "logfile", "", "zookeeper logfile");

    FILE *fp = fopen(_zoo_logfile.c_str(), "a");
    if (fp != nullptr)
        zoo_set_log_stream(fp);
}

zookeeper_session *zookeeper_session_mgr::get_session(const service_app_info &info)
{
    auto &store = utils::singleton_store<int, zookeeper_session *>::instance();
    zookeeper_session *ans = nullptr;
    utils::auto_lock<utils::ex_lock_nr> l(_store_lock);
    if (!store.get(info.entity_id, ans)) {
        ans = new zookeeper_session(info);
        store.put(info.entity_id, ans);
    }
    return ans;
}
}
}
