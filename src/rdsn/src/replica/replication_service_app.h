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

#include "utils/autoref_ptr.h"
#include "runtime/service_app.h"

namespace dsn {

class version_http_service;

namespace replication {

namespace test {
class test_checker;
}

class replica_stub;
typedef dsn::ref_ptr<replica_stub> replica_stub_ptr;

class replication_service_app : public ::dsn::service_app
{
public:
    static void register_all();

    replication_service_app(const dsn::service_app_info *info);

    virtual ~replication_service_app(void);

    virtual ::dsn::error_code start(const std::vector<std::string> &args) override;

    virtual ::dsn::error_code stop(bool cleanup = false) override;

    virtual void
    on_intercepted_request(dsn::gpid gpid, bool is_write, dsn::message_ex *msg) override;

private:
    friend class ::dsn::replication::test::test_checker;
    replica_stub_ptr _stub;
};
} // namespace replication
} // namespace dsn
