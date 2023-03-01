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

#include "utils/singleton_store.h"
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
#include "runtime/rpc/rpc_address.h"
#include <string>

#pragma once

namespace dsn {
namespace dist {

class zookeeper_session;
class zookeeper_session_mgr : public utils::singleton<zookeeper_session_mgr>
{
public:
    zookeeper_session *get_session(const service_app_info &info);

private:
    zookeeper_session_mgr();
    ~zookeeper_session_mgr() = default;

    friend class utils::singleton<zookeeper_session_mgr>;
};
} // namespace dist
} // namespace dsn
