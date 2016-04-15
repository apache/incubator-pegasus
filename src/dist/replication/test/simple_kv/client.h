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
 *     Replication testing framework.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include "simple_kv.client.h"

namespace dsn { namespace replication { namespace test {

class simple_kv_client_app : public ::dsn::service_app, public virtual ::dsn::clientlet
{
public:
    simple_kv_client_app();
    virtual ~simple_kv_client_app();

    virtual ::dsn::error_code start(int argc, char** argv) override;
    virtual ::dsn::error_code stop(bool cleanup = false) override;

    void run();

    void begin_read(int id, const std::string& key, int timeout_ms);
    void begin_write(int id,const std::string& key,const std::string& value, int timeout_ms);
    void send_config_to_meta(const rpc_address& receiver, dsn::replication::config_type::type type, const rpc_address& node);
private:
    std::unique_ptr<simple_kv_client> _simple_kv_client;
    rpc_address _meta_server_group;
    url_host_address _service_addr;
};

}}}

