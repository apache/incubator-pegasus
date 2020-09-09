// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/errors.h>
#include <dsn/utility/flags.h>

namespace dsn {

DSN_DECLARE_bool(enable_http_server);

enum http_method
{
    HTTP_METHOD_GET = 1,
    HTTP_METHOD_POST = 2,
};

class message_ex;
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
};

typedef std::function<void(const http_request &req, http_response &resp)> http_callback;

// Defines the structure of an HTTP call.
struct http_call
{
    std::string path;
    std::string help;
    http_callback callback;

    http_call &with_callback(http_callback cb)
    {
        callback = std::move(cb);
        return *this;
    }
    http_call &with_help(std::string hp)
    {
        help = std::move(hp);
        return *this;
    }
};

// A suite of HTTP handlers coupled using the same prefix of the service.
// If a handler is registered with path 'app/duplication', its real path is
// "/<root_path>/app/duplication".
class http_service
{
public:
    virtual ~http_service() = default;

    virtual std::string path() const = 0;

    void register_handler(std::string path, http_callback cb, std::string help);
};

// Example:
//
// ```
// register_http_call("/meta/app")
//     .with_callback(std::bind(&meta_http_service::get_app_handler,
//                              this,
//                              std::placeholders::_1,
//                              std::placeholders::_2))
//     .with_help("Gets the app information")
//     .add_argument("app_name", HTTP_ARG_STRING);
// ```
extern http_call &register_http_call(std::string full_path);

// Starts serving HTTP requests.
// The internal HTTP server will reuse the rDSN server port.
extern void start_http_server();

// NOTE: the memory of `svc` will be transferred to the underlying registry.
// TODO(wutao): pass `svc` as a std::unique_ptr.
extern void register_http_service(http_service *svc);

} // namespace dsn
