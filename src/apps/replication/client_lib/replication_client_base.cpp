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
 *     interface for clients operations using rDSN
 *
 * Revision history:
 *     Dec., 2015, @xiaotz (Xiaotong Zhang), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include <dsn/service_api_cpp.h>
# include <dsn/dist/replication.h>

using namespace dsn::service;

namespace dsn { namespace replication {

replication_client_base::replication_client_base(
        const std::vector<dsn::rpc_address>& meta_servers
        )
{
    _meta_servers = meta_servers;
}

replication_client_base::~replication_client_base()
{
    zauto_write_lock l(_config_lock);
    for (auto& app_client : _app_clients)
    {
        replication_app_client_base* client_base = app_client.second;
        delete client_base;
    }
    _app_clients.clear();
}

replication_app_client_base* replication_client_base::get_app_client_base(const char* app_name)
{
    zauto_write_lock l(_config_lock);
    auto it = _app_clients.find(app_name);
    if(it == _app_clients.end())
    {
        replication_app_client_base* client_base = new replication_app_client_base(_meta_servers, app_name);
        it = _app_clients.insert(std::unordered_map<std::string, replication_app_client_base*>::value_type(app_name, client_base)).first;
    }
    return it->second;
}

}} // namespace
