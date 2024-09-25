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

#include <memory>
#include <string>
#include <vector>

#include "meta_admin_types.h"
#include "rpc/rpc_host_port.h"
#include "runtime/service_app.h"
#include "task/task_tracker.h"
#include "utils/error_code.h"

namespace dsn {

namespace replication {
namespace application {
class simple_kv_client;
} // namespace application

namespace test {

class simple_kv_client_app : public ::dsn::service_app
{
public:
    simple_kv_client_app(const service_app_info *info);
    virtual ~simple_kv_client_app();

    virtual ::dsn::error_code start(const std::vector<std::string> &args) override;
    virtual ::dsn::error_code stop(bool cleanup = false) override;

    void run();

    void begin_read(int id, const std::string &key, int timeout_ms);
    void begin_write(int id, const std::string &key, const std::string &value, int timeout_ms);
    void send_config_to_meta(const host_port &receiver,
                             dsn::replication::config_type::type type,
                             const host_port &node);

private:
    std::unique_ptr<application::simple_kv_client> _simple_kv_client;
    host_port _meta_server_group;
    host_port _service_addr;
    dsn::task_tracker _tracker;
};
} // namespace test
} // namespace replication
} // namespace dsn
