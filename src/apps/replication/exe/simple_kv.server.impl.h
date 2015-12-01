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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include "simple_kv.server.h"

namespace dsn {
    namespace replication {
        namespace application {
            class simple_kv_service_impl : public simple_kv_service
            {
            public:
                simple_kv_service_impl(replica* replica);

                // RPC_SIMPLE_KV_READ
                virtual void on_read(const std::string& key, ::dsn::rpc_replier<std::string>& reply);
                // RPC_SIMPLE_KV_WRITE
                virtual void on_write(const kv_pair& pr, ::dsn::rpc_replier<int32_t>& reply);
                // RPC_SIMPLE_KV_APPEND
                virtual void on_append(const kv_pair& pr, ::dsn::rpc_replier<int32_t>& reply);

                virtual int  open(bool create_new);
                virtual int  close(bool clear_state);
                virtual int  checkpoint();

                // helper routines to accelerate learning
                virtual int get_checkpoint(decree start, const blob& learn_req, /*out*/ learn_state& state);
                virtual int apply_checkpoint(learn_state& state, chkpt_apply_mode mode);

            private:
                void recover();
                void recover(const std::string& name, decree version);

            private:
                typedef std::map<std::string, std::string> simple_kv;
                simple_kv _store;
                ::dsn::service::zlock _lock;
                bool      _test_file_learning;
            };

        }
    }
} // namespace
