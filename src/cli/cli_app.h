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

# include <dsn/serverlet.h>
# include "cli.client.h"

namespace dsn {
    namespace service {
#define MAX_PJS_REQUEST 20
        class cli_pjs
        {
        public:
            dsn::task_ptr timer;
            command rcmd;
            command topcmd;
            std::string chart_type;
            std::string profile_view;
            int count;
            int interval;
        };

        class cli : public service_app, public virtual ::dsn::service::servicelet
        {
        public:
            cli(service_app_spec* s);
            virtual error_code start(int argc, char** argv);
            virtual std::string trans_to_json_file(std::string src, int nowreq);
            virtual void get_pjs_json_file(int nowreq);
            virtual bool get_pjs_top_data(int nowreq);
            virtual void stop(bool cleanup = false);

        private:
            cli_pjs    _pjs_request[MAX_PJS_REQUEST];
            cli_client _client;
            end_point  _target;
            int        _timeout_seconds;
        };
    }
}
