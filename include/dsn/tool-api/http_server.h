// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/c/api_common.h>
#include <dsn/tool-api/rpc_message.h>
#include <dsn/cpp/serverlet.h>
#include <dsn/utility/errors.h>

namespace dsn {

enum http_method
{
    HTTP_METHOD_GET = 1,
    HTTP_METHOD_POST = 2,
};

struct http_request
{
    static error_with<http_request> parse(dsn::message_ex *m);

    // http://ip:port/<service>/<method>
    std::pair<std::string, std::string> service_method;
    // <args_name, args_val>
    std::unordered_map<std::string, std::string> query_args;
    blob body;
    blob full_url;
    http_method method;
};

enum class http_status_code
{
    ok,                    // 200
    temporary_redirect,    // 307
    bad_request,           // 400
    not_found,             // 404
    internal_server_error, // 500
};

extern std::string http_status_code_to_string(http_status_code code);

struct http_response
{
    std::string body;
    http_status_code status_code{http_status_code::ok};
    std::string content_type = "text/plain";
    std::string location;

    message_ptr to_message(message_ex *req) const;
};

class http_service
{
public:
    typedef std::function<void(const http_request &req, http_response &resp)> http_callback;

    virtual ~http_service() = default;

    virtual std::string path() const = 0;

    void register_handler(std::string path, http_callback cb, std::string help)
    {
        _cb_map.emplace(std::move(path), std::make_pair(std::move(cb), std::move(help)));
    }

    void call(const http_request &req, http_response &resp)
    {
        auto it = _cb_map.find(req.service_method.second);
        if (it != _cb_map.end()) {
            it->second.first(req, resp);
        } else {
            resp.status_code = http_status_code::not_found;
            resp.body = std::string("method not found for \"") + req.service_method.second + "\"";
        }
    }

    struct method_help_entry
    {
        std::string name;
        std::string help;
    };
    std::vector<method_help_entry> get_help() const
    {
        std::vector<method_help_entry> ret;
        ret.reserve(_cb_map.size());
        for (const auto &method : _cb_map) {
            ret.push_back({method.first, method.second.second});
        }
        return ret;
    }

private:
    std::map<std::string, std::pair<http_callback, std::string>> _cb_map;
};

class http_server : public serverlet<http_server>
{
public:
    explicit http_server(bool start = true);

    ~http_server() override = default;

    void add_service(http_service *service);

    void serve(message_ex *msg);

    struct service_method_help_entry
    {
        std::string name;
        std::string method;
        std::string help;
    };
    std::vector<service_method_help_entry> get_help() const
    {
        std::vector<service_method_help_entry> ret;
        for (const auto &service : _service_map) {
            for (const auto &method : service.second->get_help()) {
                ret.push_back({service.first, method.name, method.help});
            }
        }
        return ret;
    }

private:
    std::map<std::string, std::unique_ptr<http_service>> _service_map;
};

/// The rpc code for all the HTTP RPCs.
/// Since http is used only for system monitoring, it is restricted to lowest priority.
DEFINE_TASK_CODE_RPC(RPC_HTTP_SERVICE, TASK_PRIORITY_LOW, THREAD_POOL_DEFAULT);

} // namespace dsn
