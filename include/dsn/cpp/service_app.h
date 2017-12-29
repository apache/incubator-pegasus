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
 *     application model atop zion in c++
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/service_api_c.h>
#include <dsn/cpp/auto_codes.h>
#include <dsn/cpp/address.h>
#include <vector>
#include <string>

namespace dsn {
/*!
@addtogroup app-model
@{
*/

class service_app
{
public:
    service_app(dsn_gpid gpid) : _started(false), _gpid(gpid) {}

    virtual ~service_app(void) {}

    virtual ::dsn::error_code start(int argc, char **argv) = 0;

    virtual ::dsn::error_code stop(bool cleanup = false) = 0;

    virtual void on_intercepted_request(dsn_gpid gpid, bool is_write, dsn_message_t msg)
    {
        dassert(false, "not supported");
    }
    //
    // inquery routines
    //
    bool is_started() const { return _started; }

    ::dsn::rpc_address primary_address() const { return _address; }

    const std::string &name() const { return _name; }

    dsn_gpid get_gpid() const { return _gpid; }

private:
    bool _started;
    ::dsn::rpc_address _address;
    std::string _name;
    dsn_gpid _gpid;

public:
    template <typename TServiceApp>
    static void *app_create(const char * /*tname*/, dsn_gpid gpid)
    {
        auto svc = new TServiceApp(gpid);
        return (void *)(dynamic_cast<service_app *>(svc));
    }

    static dsn_error_t app_start(void *app, int argc, char **argv)
    {
        service_app *sapp = (service_app *)app;
        sapp->_address = dsn_primary_address();
        sapp->_name = std::string(argv[0]);

        auto r = sapp->start(argc, argv);
        if (r == ::dsn::ERR_OK) {
            sapp->_started = true;
        }
        return r;
    }

    static dsn_error_t app_destroy(void *app, bool cleanup)
    {
        service_app *sapp = (service_app *)(app);
        auto err = sapp->stop(cleanup);
        if (ERR_OK == err)
            sapp->_started = false;
        return err;
    }

    static void on_intercepted_request(void *app, dsn_gpid gpid, bool is_write, dsn_message_t msg)
    {
        auto sapp = (service_app *)app;
        return sapp->on_intercepted_request(gpid, is_write, msg);
    }
};

/*! C++ wrapper of the \ref dsn_register_app function*/
template <typename TServiceApp>
void register_app(const char *type_name)
{
    dsn_app app;
    memset(&app, 0, sizeof(app));
    strncpy(app.type_name, type_name, sizeof(app.type_name));
    app.create = service_app::app_create<TServiceApp>;
    app.start = service_app::app_start;
    app.destroy = service_app::app_destroy;
    app.intercepted_request = service_app::on_intercepted_request;

    dsn_register_app(&app);
}

/*@}*/
} // end namespace dsn::service
