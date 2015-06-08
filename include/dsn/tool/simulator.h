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

#include <dsn/tool_api.h>

namespace dsn { namespace service { class service_app;  } }

namespace dsn { namespace tools {

class checker
{
public:
    checker(const char* name);

    virtual bool check() = 0;

    const std::string& name() const { return _name; }

protected:
    const std::map<std::string, ::dsn::service::service_app*>& _apps;

private:
    std::string _name;
};

class simulator : public tool_app
{
public:
    simulator(const char* name)
        : tool_app(name)
    {
    }

    void install(service_spec& s);
    
    virtual void run() override;

    template<typename T>
    void add_checker(const char* name);

private:
    void add_checker(checker* chker);
};

// ---- inline implementation ------

template<typename T>
inline void simulator::add_checker(const char* name)
{
    checker* chker = static_cast<checker*>(new T(name));
    add_checker(chker);
}

}} // end namespace dsn::tools
