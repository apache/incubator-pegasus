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

# include <dsn/service_api_c.h>
# include <vector>
# include <string>

namespace dsn 
{
    class service_app
    {
    public:
        service_app() : _started(false) { _address = dsn_address_invalid; }

        virtual ~service_app(void) {}

        virtual ::dsn::error_code start(int argc, char** argv) = 0;

        virtual void stop(bool cleanup = false) = 0;

        bool is_started() const { return _started; }

        const dsn_address_t& primary_address() const { return _address; }

        const std::string& name() const { return _name; }

    private:
        void register_for_debugging();

    private:
        bool          _started;
        dsn_address_t _address;
        std::string   _name;

    public:
        template<typename TServiceApp>
        static void* app_create()
        {
            auto svc =  new TServiceApp();
            return (void*)(dynamic_cast<service_app*>(svc));
        }

        static dsn_error_t app_start(void* app, int argc, char** argv)
        {
            service_app* sapp = (service_app*)app;
            auto r = sapp->start(argc, argv);
            if (r == ::dsn::ERR_OK)
            {
                sapp->_started = true;
                sapp->_address = dsn_primary_address();
                sapp->_name = std::string(argv[0]);
                sapp->register_for_debugging();
            }
            return r;
        }

        static void app_destroy(void* app, bool cleanup)
        {
            service_app* sapp = (service_app*)(app);
            sapp->stop(cleanup);
            sapp->_started = false;
        }
    };

    template<typename TServiceApp>
    void register_app(const char* type_name)
    {
        dsn_register_app_role(type_name, service_app::app_create<TServiceApp>, service_app::app_start, service_app::app_destroy);
    }
} // end namespace dsn::service

