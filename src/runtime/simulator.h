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

#include <string>
#include <vector>

#include "runtime/tool_api.h"
#include "utils/sys_exit_hook.h"

namespace dsn {
class service_app;
struct service_spec;

namespace tools {

class checker
{
public:
    typedef checker *(*factory)();
    template <typename T>
    static checker *create()
    {
        return new T();
    }

public:
    checker() {}
    virtual ~checker() {}
    virtual void initialize(const std::string &name, const std::vector<service_app *> &apps) = 0;
    virtual void check() = 0;
    const std::string &name() const { return _name; }
protected:
    std::vector<service_app *> _apps;
    std::string _name;
};

class simulator : public tool_app
{
public:
    simulator(const char *name) : tool_app(name) {}
    virtual void install(service_spec &s) override;
    virtual void run() override;
    static void register_checker(const std::string &name, checker::factory f);

private:
    static void on_system_exit(sys_exit_type st);
};

// ---- inline implementation ------
}
} // end namespace dsn::tools
