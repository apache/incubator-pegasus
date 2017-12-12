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

#include "rpc_engine.h"
#include <dsn/utility/singleton.h>
#include <unordered_map>
#include <dsn/utility/synchronize.h>
#include <dsn/utility/configuration.h>
#include <dsn/utility/factory_store.h>
#include <dsn/tool-api/task.h>
#include <dsn/tool-api/group_address.h>
#include <dsn/tool-api/uri_address.h>

namespace dsn {
void uri_resolver_manager::setup_resolvers()
{
    // [uri-resolver.%resolver-address%]
    // factory = %uri-resolver-factory%
    // arguments = %uri-resolver-arguments%

    std::vector<std::string> sections;
    get_main_config()->get_all_sections(sections);

    const int prefix_len = (const int)strlen("uri-resolver.");
    for (auto &s : sections) {
        if (s.substr(0, prefix_len) != std::string("uri-resolver."))
            continue;

        auto resolver_addr = s.substr(prefix_len);
        auto it = _resolvers.find(resolver_addr);
        if (it != _resolvers.end()) {
            dwarn("duplicated uri-resolver definition in config file, for '%s'",
                  resolver_addr.c_str());
            continue;
        }

        auto factory = dsn_config_get_value_string(
            s.c_str(),
            "factory",
            "",
            "partition-resolver factory name which creates the concrete partition-resolver object");
        auto arguments =
            dsn_config_get_value_string(s.c_str(), "arguments", "", "uri-resolver ctor arguments");

        auto resolver = new uri_resolver(resolver_addr.c_str(), factory, arguments);
        _resolvers.emplace(resolver_addr, std::shared_ptr<uri_resolver>(resolver));

        dinfo("initialize uri-resolver %s", resolver_addr.c_str());
    }
}

std::shared_ptr<uri_resolver> uri_resolver_manager::get(rpc_uri_address *uri) const
{
    std::shared_ptr<uri_resolver> ret = nullptr;
    auto pr = uri->get_uri_components();
    if (pr.first.length() == 0)
        return ret;

    {
        utils::auto_read_lock l(_lock);
        auto it = _resolvers.find(pr.first);
        if (it != _resolvers.end())
            ret = it->second;
    }

    dassert(ret != nullptr,
            "cannot find uri resolver for uri '%s' with resolver address as '%s', "
            "please fix it by setting up a uri resolver section in config file, as follows:\n"
            "[uri-resolver.%s]\n"
            "factory = partition-resolver-factory (e.g., partition_resolver_simple)\n"
            "arguments = uri-resolver-arguments (e.g., localhost:34601,localhost:34602)\n",
            uri->uri(),
            pr.first.c_str(),
            pr.first.c_str());

    return ret;
}

std::map<std::string, std::shared_ptr<uri_resolver>> uri_resolver_manager::get_all() const
{
    std::map<std::string, std::shared_ptr<uri_resolver>> result;

    {
        utils::auto_read_lock l(_lock);
        result.insert(_resolvers.begin(), _resolvers.end());
    }

    return result;
}

//---------------------------------------------------------------

rpc_uri_address::rpc_uri_address(const char *uri) : _uri(uri)
{
    auto r1 = task::get_current_rpc()->uri_resolver_mgr()->get(this);
    if (r1.get()) {
        _resolver = r1->get_app_resolver(get_uri_components().second.c_str());
    }
}

rpc_uri_address::rpc_uri_address(const rpc_uri_address &other)
{
    _resolver = other._resolver;
    _uri = other._uri;
}

rpc_uri_address &rpc_uri_address::operator=(const rpc_uri_address &other)
{
    _resolver = other._resolver;
    _uri = other._uri;
    return *this;
}

rpc_uri_address::~rpc_uri_address() { _resolver = nullptr; }

std::pair<std::string, std::string> rpc_uri_address::get_uri_components()
{
    auto it = _uri.find("://");
    if (it == std::string::npos)
        return std::make_pair("", "");

    auto it2 = _uri.find('/', it + 3);
    if (it2 == std::string::npos)
        return std::make_pair(_uri, "");

    else
        return std::make_pair(_uri.substr(0, it2), _uri.substr(it2 + 1));
}

//---------------------------------------------------------------

uri_resolver::uri_resolver(const char *name, const char *factory, const char *arguments)
    : _name(name), _factory(factory), _arguments(arguments)
{
    _meta_server.assign_group(name);

    std::vector<std::string> args;
    utils::split_args(arguments, args, ',');
    for (auto &arg : args) {
        // name:port
        auto pos1 = arg.find_first_of(':');
        if (pos1 != std::string::npos) {
            ::dsn::rpc_address ep(arg.substr(0, pos1).c_str(), atoi(arg.substr(pos1 + 1).c_str()));
            _meta_server.group_address()->add(ep);
        }
    }
}

uri_resolver::~uri_resolver() {}

dist::partition_resolver_ptr uri_resolver::get_app_resolver(const char *app)
{
    dist::partition_resolver_ptr rv = nullptr;

    {
        service::zauto_read_lock l(_apps_lock);
        auto it = _apps.find(app);
        if (it != _apps.end()) {
            rv = it->second;
        }
    }

    // create on demand
    if (rv == nullptr) {
        // here we can pass _meta_server to partition_resolver without clone,
        // because the lift time of uri_resolver covers that of partition_resolver.
        rv = utils::factory_store<dist::partition_resolver>::create(
            _factory.c_str(), ::dsn::PROVIDER_TYPE_MAIN, _meta_server, app);

        service::zauto_write_lock l(_apps_lock);
        auto it = _apps.find(app);
        if (it == _apps.end()) {
            _apps.emplace(std::string(app), rv);
        } else {
            rv = it->second;
        }
    }

    return rv;
}

std::map<std::string, dist::partition_resolver_ptr> uri_resolver::get_all_app_resolvers()
{
    std::map<std::string, dist::partition_resolver_ptr> result;

    {
        service::zauto_read_lock l(_apps_lock);
        result.insert(_apps.begin(), _apps.end());
    }

    return result;
}
}
