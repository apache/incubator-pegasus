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

#include <functional>
#include <memory>
#include <vector>

#include "failure_detector/failure_detector.h"
#include "runtime/rpc/group_host_port.h"
#include "runtime/rpc/rpc_host_port.h"
#include "utils/fmt_logging.h"
#include "utils/zlocks.h"

namespace dsn {
class error_code;

namespace fd {
class beacon_ack;
} // namespace fd

namespace dist {

class slave_failure_detector_with_multimaster : public dsn::fd::failure_detector
{
public:
    slave_failure_detector_with_multimaster(std::vector<::dsn::host_port> &meta_servers,
                                            std::function<void()> &&master_disconnected_callback,
                                            std::function<void()> &&master_connected_callback);
    virtual ~slave_failure_detector_with_multimaster() {}

    void end_ping(::dsn::error_code err, const fd::beacon_ack &ack, void *context) override;

    // client side
    void on_master_disconnected(const std::vector<::dsn::host_port> &nodes) override;
    void on_master_connected(::dsn::host_port node) override;

    // server side
    void on_worker_disconnected(const std::vector<::dsn::host_port> &nodes) override
    {
        CHECK(false, "invalid execution flow");
    }
    void on_worker_connected(::dsn::host_port node) override
    {
        CHECK(false, "invalid execution flow");
    }

    ::dsn::host_port current_server_contact() const;
    host_port get_servers() const { return _meta_servers; }

    void set_leader_for_test(dsn::host_port meta);

private:
    host_port _meta_servers;
    std::function<void()> _master_disconnected_callback;
    std::function<void()> _master_connected_callback;
};

//------------------ inline implementation --------------------------------
inline ::dsn::host_port slave_failure_detector_with_multimaster::current_server_contact() const
{
    zauto_lock l(failure_detector::_lock);
    return _meta_servers.group_host_port()->leader();
}
}
} // end namespace
