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


# include "replication_common.h"
# include "replica_stub.h"

namespace dsn { namespace replication {

replication_service_app::replication_service_app()
    : service_app()
{
    _stub = nullptr;

#if 0
    ::boost::filesystem::path pr("./");
    if (!::boost::filesystem::exists(pr))
    {
        printf("we are avoiding a bug in boost file system here\n");
        // http://boost.2283326.n4.nabble.com/filesystem-v3-v1-49-Path-constructor-issue-in-VS-Debug-configuration-td4463703.html
    }
#endif
}

replication_service_app::~replication_service_app(void)
{
}

error_code replication_service_app::start(int argc, char** argv)
{
    replication_options opts;
    opts.initialize();
    opts.working_dir = std::string("./") + argv[0];

    _stub = new replica_stub();
    _stub->initialize(opts);
    _stub->open_service();
    return ERR_OK;
}

void replication_service_app::stop(bool cleanup)
{
    if (_stub != nullptr)
    {
        _stub->close();
        _stub = nullptr;
    }
}

}}
