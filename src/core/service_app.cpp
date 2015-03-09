/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
# include <rdsn/internal/service_app.h>
# include <rdsn/service_api.h>

namespace rdsn { namespace service {

service_app::service_app(service_app_spec* s, configuration_ptr c)
{
    _spec = *s;
    _config = c;

    std::vector<std::string> args;
    utils::split_args(_spec.arguments.c_str(), args);

    int argc = (int)args.size() + 1;
    _argsPtr.resize(argc);
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

        _argsPtr[i] = ((char*)_args[i].c_str());
    }

    _address.port = (uint16_t)_spec.port;
}

service_app::~service_app(void)
{
}

void service_app::set_address(const end_point& addr)
{
    rassert (_address.port == 0 || _address.port == addr.port, "invalid service address");
    _address = addr;
}

void service_apps::add(service_app* app)
{
    _apps[app->name()] = app;
}
    
service_app* service_apps::get(const char* name) const
{
    auto it = _apps.find(name);
    if (it != _apps.end())
        return it->second;
    else
        return nullptr;
}

}} // end namespace rdsn::service_api
