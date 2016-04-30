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

#include "simple_kv.server.h"
# include <dsn/cpp/replicated_service_app.h>

namespace dsn {
    namespace replication {
        namespace test {

            class simple_kv_service_impl :
                public simple_kv_service,
                public replicated_service_app_type_1
            {
            public:
                static bool s_simple_kv_open_fail;
                static bool s_simple_kv_close_fail;
                static bool s_simple_kv_get_checkpoint_fail;
                static bool s_simple_kv_apply_checkpoint_fail;

            public:
                simple_kv_service_impl();

                // RPC_SIMPLE_KV_READ
                virtual void on_read(const std::string& key, ::dsn::rpc_replier<std::string>& reply);
                // RPC_SIMPLE_KV_WRITE
                virtual void on_write(const kv_pair& pr, ::dsn::rpc_replier<int32_t>& reply);
                // RPC_SIMPLE_KV_APPEND
                virtual void on_append(const kv_pair& pr, ::dsn::rpc_replier<int32_t>& reply);

                virtual ::dsn::error_code start(int argc, char** argv) override;

                virtual ::dsn::error_code stop(bool cleanup = false) override;

                virtual ::dsn::error_code checkpoint(int64_t version) override;

                virtual ::dsn::error_code checkpoint_async(int64_t version) override;

                virtual int64_t get_last_checkpoint_version() const override { return last_durable_decree(); }

                virtual int prepare_get_checkpoint(void* buffer, int capacity) override { return 0; }

                virtual ::dsn::error_code get_checkpoint(
                    int64_t start,
                    void*   learn_request,
                    int     learn_request_size,
                    /* inout */ app_learn_state& state
                    ) override;

                virtual ::dsn::error_code apply_checkpoint(const dsn_app_learn_state& state, dsn_chkpt_apply_mode mode) override;

            private:
                void recover();
                void recover(const std::string& name, int64_t version);
                const char* data_dir() const { return _data_dir.c_str(); }
                int64_t last_durable_decree() const { return _checkpoint_version; }
                void    set_last_durable_decree(int64_t d) { _checkpoint_version = d; }

            private:
                typedef std::map<std::string, std::string> simple_kv;
                simple_kv _store;
                ::dsn::service::zlock _lock;
                bool _test_file_learning;

                std::string _data_dir;
                int64_t     _checkpoint_version;
            };

        }
    }
}

