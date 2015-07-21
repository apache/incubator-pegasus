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
# include <dsn/internal/service_app.h>
# include <dsn/service_api.h>
# include "service_engine.h"
# include "rpc_engine.h"

namespace dsn { namespace service {

service_app::service_app(service_app_spec* s)
{
    _started = false;
    _spec = *s;

    std::vector<std::string> args;
    utils::split_args(_spec.arguments.c_str(), args);

    int argc = static_cast<int>(args.size()) + 1;
    _args_ptr.resize(argc);
    _args.resize(argc);
    for (int i = 0; i < argc; i++)
    {
        if (0 == i)
        {
            _args[0] = _spec.type;
        }
        else
        {
            _args[i] = args[i-1];
        }

        _args_ptr[i] = ((char*)_args[i].c_str());
    }
}

service_app::~service_app(void)
{
}

const dsn_address_t& service_app::primary_address() const
{
    return _svc_node->rpc()->primary_address();
}

}} // end namespace dsn::service_api
