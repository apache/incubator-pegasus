/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus(rDSN) -=- 
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

# include "counter.server.h"

namespace dsn {
    namespace example {

        using namespace ::dsn::replication;

        class counter_service_impl
            : public counter_service
        {
        public:
            counter_service_impl(replica* replica, configuration_ptr& config);

            virtual void on_add(const ::dsn::example::count_op& op, ::dsn::service::rpc_replier<int32_t>& reply) override;
            virtual void on_read(const std::string& name, ::dsn::service::rpc_replier<int32_t>& reply) override;

            //
            // interfaces to be implemented by app
            // all return values are error code
            //
            virtual int  open(bool create_new); // singel threaded
            virtual int  close(bool clear_state); // must be thread-safe

            // update _last_durable_decree internally
            virtual int  flush(bool force);  // must be thread-safe

            //
            // helper routines to accelerate learning
            // 
            virtual int  get_learn_state(decree start, const blob& learn_request, __out_param learn_state& state);  // must be thread-safe
            virtual int  apply_learn_state(learn_state& state);  // must be thread-safe, and last_committed_decree must equal to last_durable_decree after learning

        private:
            void recover();
            void recover(const std::string& name, decree version);

        private:
            ::dsn::service::zlock _lock;
            std::map<std::string, int32_t> _counters;
        };
    }
}

