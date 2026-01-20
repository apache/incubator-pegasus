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

#include <zookeeper/zookeeper.h>
#include <cerrno>
#include <cstdio>
#include <functional>

#include "runtime/service_app.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/safe_strerror_posix.h"
#include "utils/singleton_store.h"
#include "zookeeper_session.h"

DSN_DEFINE_string(zookeeper, logfile, "zoo.log", "The path of the log file for ZooKeeper C client");

namespace dsn::dist {

zookeeper_session_mgr::zookeeper_session_mgr()
{
    FILE *zoo_log_file = std::fopen(FLAGS_logfile, "a");
    if (zoo_log_file == nullptr) {
        LOG_ERROR("failed to open the log file for ZooKeeper C Client: path = {}, error = {}",
                  utils::safe_strerror(errno));
    } else {
        // The file handle pointed to by `zoo_log_file` will never be released because:
        // 1. `zookeeper_session_mgr` is a singleton, so each meta server process contains
        // holds only one log file handle.
        // 2. It cannot be guaranteed that the handle will no longer be used after being
        // released. Even if it is closed in a different place (instead of in the destructor
        // of `zookeeper_session_mgr`), there is still no guarantee. Once `fclose` is called
        // on the log file handle, any subsequent read or write operations on it would result
        // in undefined behavior.
        zoo_set_log_stream(zoo_log_file);
    }
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

} // namespace dsn::dist
