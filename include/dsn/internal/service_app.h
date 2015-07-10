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
# pragma once

# include <dsn/internal/error_code.h>
# include <dsn/internal/end_point.h>
# include <dsn/internal/configuration.h>
# include <dsn/internal/global_config.h>
# include <string>

namespace dsn { 
class service_node;    
class service_control_task;

namespace service {

class service_app
{
public:
    template <typename T> static service_app* create(service_app_spec* s)
    {
        return new T(s);
    }


public:
    service_app(service_app_spec* s);
    virtual ~service_app(void);

    virtual error_code start(int argc, char** argv) = 0;

    virtual void stop(bool cleanup = false) = 0;

    const service_app_spec& spec() const { return _spec; }
    const std::string& name() const { return _spec.name; }
    int arg_count() const { return static_cast<int>(_args.size()); }
    char** args() const { return (char**)&_args_ptr[0]; }
    service_node* node() const { return _svc_node; }
    const end_point& primary_address() const;
    int id() const { return spec().id; }
    bool is_started() const { return _started; }

private:
    friend class service_app_helper;
    void set_service_node(service_node* node) { _svc_node = node; }
        
private:    
    friend class ::dsn::service_control_task;
    std::vector<std::string> _args;
    std::vector<char*>       _args_ptr;
    service_node*            _svc_node;
    service_app_spec         _spec;
    bool                     _started;
};

typedef service_app* (*service_app_factory)(service_app_spec*);

}} // end namespace dsn::service_api


