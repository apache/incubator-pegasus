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
 *     a daemon app which managers stateless services on a machine
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/cpp/service_app.h>

namespace dsn 
{
    namespace dist 
    {
        class daemon_s_service;

        class daemon :
            public ::dsn::service_app
        {
        public:
            daemon(dsn_gpid gpid);
            ~daemon();

            virtual ::dsn::error_code start(int argc, char** argv) override;

            virtual ::dsn::error_code stop(bool cleanup = false) override;

        private:
            std::unique_ptr<daemon_s_service> _daemon_s_svc;
        };
    }
}