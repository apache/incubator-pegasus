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

# pragma once
# include "simple_kv.client.h"

namespace dsn {
    namespace replication {
        namespace application {

            class simple_kv_perf_test_client
                : public simple_kv_client, 
                  public ::dsn::service::perf_client_helper
            {
            public:
                simple_kv_perf_test_client(
                    const std::vector< ::dsn::rpc_address>& meta_servers,
                    const char* app_name)
                    : simple_kv_client(meta_servers, app_name)
                {
                }

                void start_test()
                {
                    perf_test_suite s;
                    std::vector<perf_test_suite> suits;

                    s.name = "simple_kv.write";
                    s.config_section = "task.RPC_SIMPLE_KV_SIMPLE_KV_WRITE";
                    s.send_one = [this](int payload_bytes){this->send_one_write(payload_bytes); };
                    s.cases.clear();
                    load_suite_config(s);
                    suits.push_back(s);

                    s.name = "simple_kv.append";
                    s.config_section = "task.RPC_SIMPLE_KV_SIMPLE_KV_APPEND";
                    s.send_one = [this](int payload_bytes){this->send_one_append(payload_bytes); };
                    s.cases.clear();
                    load_suite_config(s);
                    suits.push_back(s);

                    s.name = "simple_kv.read";
                    s.config_section = "task.RPC_SIMPLE_KV_SIMPLE_KV_READ";
                    s.send_one = [this](int payload_bytes){this->send_one_read(payload_bytes); };
                    s.cases.clear();
                    load_suite_config(s);
                    suits.push_back(s);

                    start(suits);
                }

                virtual uint64_t get_key_hash(const std::string& key) override
                {
                    return dsn_crc64_compute(key.c_str(), key.size(), 0);
                }

                virtual uint64_t get_key_hash(const ::dsn::replication::application::kv_pair& key) override
                {
                    return dsn_crc64_compute(key.key.c_str(), key.key.size(), 0);
                }
                                
                void send_one_read(int payload_bytes)
                {
                    void* ctx = prepare_send_one();
                    auto rs = random64(0, 10000000);
                    std::stringstream ss;
                    ss << "key." << rs;

                    std::string req = ss.str();
                    begin_read(req, ctx, _timeout_ms);
                }

                virtual void end_read(
                    ::dsn::error_code err,
                    const std::string& resp,
                    void* context) override
                {
                    end_send_one(context, err);
                }

                void send_one_write(int payload_bytes)
                {
                    void* ctx = prepare_send_one();
                    auto rs = random64(0, 10000000);
                    std::stringstream ss;
                    ss << "key." << rs;

                    kv_pair req;
                    req.key = ss.str();
                    req.value = "abcdefghijklmnopqrstuvwxyz1234567890";

                    begin_write(req, ctx, _timeout_ms);
                }

                virtual void end_write(
                    ::dsn::error_code err,
                    const int32_t& resp,
                    void* context) override
                {
                    end_send_one(context, err);
                }

                void send_one_append(int payload_bytes)
                {
                    void* ctx = prepare_send_one();
                    auto rs = random64(0, 10000000);
                    std::stringstream ss;
                    ss << "key." << rs;

                    kv_pair req;
                    req.key = ss.str();
                    req.value = "abcdefghijklmnopqrstuvwxyz1234567890";

                    begin_append(req, ctx, _timeout_ms);
                }

                virtual void end_append(
                    ::dsn::error_code err,
                    const int32_t& resp,
                    void* context) override
                {
                    end_send_one(context, err);
                }
            };
        }
    }
}