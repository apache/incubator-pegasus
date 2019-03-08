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
    blob body;
    blob full_url;
    http_method method;
};

enum class http_status_code
{
    ok,                    // 200
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

    message_ptr to_message(message_ex *req) const;
};

class http_service
{
public:
    typedef std::function<void(const http_request &req, http_response &resp)> http_callback;

    virtual ~http_service() = default;

    virtual std::string path() const = 0;

    void register_handler(std::string path, http_callback cb)
    {
        _cb_map.emplace(std::move(path), std::move(cb));
    }

    void call(const http_request &req, http_response &resp)
    {
        auto it = _cb_map.find(req.service_method.second);
        if (it != _cb_map.end()) {
            it->second(req, resp);
        } else {
            resp.status_code = http_status_code::bad_request;
            resp.body = std::string("method not found for \"") + req.service_method.second + "\"";
        }
    }

private:
    std::map<std::string, http_callback> _cb_map;
};

class http_server : public serverlet<http_server>
{
public:
    http_server();

    ~http_server() override = default;

    void add_service(http_service *service);

    void serve(message_ex *msg);

private:
    std::map<std::string, std::unique_ptr<http_service>> _service_map;
};

/// The rpc code for all the HTTP RPCs.
/// Since http is used only for system monitoring, it is restricted to lowest priority.
DEFINE_TASK_CODE_RPC(RPC_HTTP_SERVICE, TASK_PRIORITY_LOW, THREAD_POOL_DEFAULT);

} // namespace dsn
