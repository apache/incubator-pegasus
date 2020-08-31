// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/c/api_common.h>
#include <dsn/tool-api/rpc_message.h>
#include <dsn/cpp/serverlet.h>
#include <dsn/utility/errors.h>
#include <dsn/utility/flags.h>

namespace dsn {

DSN_DECLARE_bool(enable_http_server);

enum http_method
{
    HTTP_METHOD_GET = 1,
    HTTP_METHOD_POST = 2,
};

struct http_request
{
    static error_with<http_request> parse(dsn::message_ex *m);

    std::string path;
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

typedef std::function<void(const http_request &req, http_response &resp)> http_callback;

// Defines the structure of an HTTP call.
struct http_call
{
    std::string path;
    std::string help;
    http_callback callback;
};

class http_service
{
public:
    virtual ~http_service() = default;

    virtual std::string path() const = 0;

    void register_handler(std::string path, http_callback cb, std::string help);
};

class http_server : public serverlet<http_server>
{
public:
    explicit http_server();

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
