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
 *     Replication testing framework.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "meta_admin_types.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/service_app.h"
#include "runtime/task/task_tracker.h"
#include "utils/error_code.h"

namespace pegasus {
namespace replication {
namespace application {
class simple_kv_client;
} // namespace application

namespace test {

class simple_kv_client_app : public service_app
{
public:
    simple_kv_client_app(const service_app_info *info);
    virtual ~simple_kv_client_app();

    virtual error_code start(const std::vector<std::string> &args) override;
    virtual error_code stop(bool cleanup = false) override;

    void run();

    void begin_read(int id, const std::string &key, int timeout_ms);
    void begin_write(int id, const std::string &key, const std::string &value, int timeout_ms);
    void send_config_to_meta(const rpc_address &receiver,
                             replication::config_type::type type,
                             const rpc_address &node);

private:
    std::unique_ptr<application::simple_kv_client> _simple_kv_client;
    rpc_address _meta_server_group;
    rpc_address _service_addr;
    task_tracker _tracker;
};
}
}
}
