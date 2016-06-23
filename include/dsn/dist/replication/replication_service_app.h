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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

# include <dsn/dist/layer2_handler.h>
# include <dsn/dist/replication/replication_other_types.h>

namespace dsn { namespace replication {

class replication_checker;
namespace test {
    class test_checker;
}
class replication_service_app :
    public ::dsn::layer2_handler
{
public:
    replication_service_app(dsn_gpid gpid);

    virtual ~replication_service_app(void);

    virtual ::dsn::error_code start(int argc, char** argv) override;

    virtual ::dsn::error_code stop(bool cleanup = false) override;

    virtual void on_request(dsn_gpid gpid, bool is_write, dsn_message_t msg) override;

private:
    friend class ::dsn::replication::replication_checker;
    friend class ::dsn::replication::test::test_checker;
    replica_stub_ptr _stub;

    static const char* replica_service_app_info(int argc, char** argv);
    static void replica_service_app_info_free(const char* response);
};

}}


