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

#include <dsn/tool/global_checker.h>

namespace dsn {
namespace example {

class echo_checker : public ::dsn::tools::checker
{
public:
    echo_checker(const char *name, dsn_app_info *info, int count)
        : ::dsn::tools::checker(name, info, count)
    {
        for (auto &app : _apps) {
            // TODO: identify your own type of service apps
            // if (0 == strcmp(app.second.type, "meta"))
            //{
            //    _meta_servers.push_back((meta_service_app*)app.second.app_context_ptr);
            //}
        }
    }

    virtual void check() override
    {
        // nothing to check
        // if (_meta_servers.size() == 0)
        //    return;

        // check all invariances
        /*
        auto meta = meta_leader();
        if (!meta) return;

        for (auto& r : _replica_servers)
        {
            if (!r->is_started())
                continue;

            auto ep = r->primary_address();
            if (!meta->_service->_failure_detector->is_worker_connected(ep))
            {
                dassert(!r->_stub->is_connected(), "when meta server says a replica is dead, it must
        be dead");
            }
        }
        */
    }

private:
    // std::vector<meta_service_app*>        _meta_servers;
};
}
}