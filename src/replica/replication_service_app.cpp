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

#include "replica/replication_service_app.h"

#include "common/gpid.h"
#include "common/replication_common.h"
#include "http/http_server.h"
#include "http/service_version.h"
#include "replica_http_service.h"
#include "replica_stub.h"

namespace dsn {
class message_ex;

namespace replication {

void replication_service_app::register_all()
{
    dsn::service_app::register_factory<replication_service_app>(
        replication_options::kReplicaAppType.c_str());
}

replication_service_app::replication_service_app(const service_app_info *info) : service_app(info)
{
    _stub = new replica_stub();

    // add http service
    register_http_service(new replica_http_service(_stub.get()));
    start_http_server();
}

replication_service_app::~replication_service_app(void) {}

error_code replication_service_app::start(const std::vector<std::string> &args)
{
    if (args.size() >= 2) {
        app_version.version = *(args.end() - 2);
        app_version.git_commit = *(args.end() - 1);
    }

    replication_options opts;
    opts.initialize();

    _stub->initialize(opts);
    _stub->open_service();

    return ERR_OK;
}

error_code replication_service_app::stop(bool cleanup)
{
    if (_stub != nullptr) {
        _stub->close();
        _stub = nullptr;
    }

    return ERR_OK;
}

void replication_service_app::on_intercepted_request(dsn::gpid gpid,
                                                     bool is_write,
                                                     dsn::message_ex *msg)
{
    if (is_write) {
        _stub->on_client_write(gpid, msg);
    } else {
        _stub->on_client_read(gpid, msg);
    }
}
} // namespace replication
} // namespace dsn
