/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

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
#pragma once

#include "simple_kv.replicated.service.h"

namespace dsn {
    namespace replication {
        namespace application {

            extern replication_app_base* create_simplekv_app(replica* replica, configuration_ptr config);

            class replication_app_example1_config : public replication_app_config
            {
            public:
                virtual bool initialize(configuration_ptr config)
                {
                    // TODO: read configs 
                    return true;
                }
            };

            class replication_app_example1 : public simple_kv_service<replication_app_example1>
            {
            public:
                replication_app_example1(replica* replica, const replication_app_config* config);

                // RPC_SIMPLE_KV_READ
                virtual void on_read(const std::string& key, ::dsn::service::rpc_replier<std::string>& reply);
                // RPC_SIMPLE_KV_WRITE
                virtual void on_write(const kv_pair& pr, ::dsn::service::rpc_replier<int32_t>& reply);
                // RPC_SIMPLE_KV_APPEND
                virtual void on_append(const kv_pair& pr, ::dsn::service::rpc_replier<int32_t>& reply);

                virtual int  open(bool create_new);
                virtual int  close(bool clear_state);
                virtual int  compact(bool force);

                // helper routines to accelerate learning
                virtual int get_learn_state(decree start, const blob& learnRequest, __out_param learn_state& state);
                virtual int apply_learn_state(learn_state& state);

            private:
                void recover();
                void recover(const std::string& name, decree version);

            private:
                typedef std::map<std::string, std::string> SimpleKV;
                SimpleKV _store;
                zlock    _lock;
                std::string _learnFileName;
            };

        }
    }
} // namespace
