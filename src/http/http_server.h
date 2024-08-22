/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "fmt/core.h"
#include "http/http_method.h"
#include "http/http_status_code.h"
#include "runtime/task/task_code.h"
#include "utils/blob.h"
#include "utils/errors.h"
#include "utils/flags.h"
#include "utils/threadpool_code.h"

DSN_DECLARE_bool(enable_http_server);

namespace dsn {

/// The rpc code for all the HTTP RPCs.
DEFINE_TASK_CODE_RPC(RPC_HTTP_SERVICE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT);

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

struct http_response
{
    std::string body;
    http_status_code status_code{http_status_code::kOk};
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
    http_call &with_help(std::string hp) { return with_help("", std::move(hp)); }
    http_call &with_help(std::string parameters, std::string hp)
    {
        help = fmt::format("{}{}{}", parameters, parameters.empty() ? "" : ". ", hp);
        return *this;
    }
};

// A suite of HTTP handlers coupled using the same prefix of the service.
// If a handler is registered with path 'app/duplication', its real path is
// "/<root_path>/app/duplication".
class http_service
{
public:
    http_service() noexcept = default;
    virtual ~http_service() = default;

    virtual std::string path() const = 0;

    void register_handler(std::string sub_path, http_callback cb, std::string help) const;

    void register_handler(std::string sub_path,
                          http_callback cb,
                          std::string parameters,
                          std::string help) const;

    void deregister_handler(std::string sub_path) const;

private:
    // If sub_path is 'app/duplication', the built path would be '<root_path>/app/duplication',
    // where path() would be called as root_path.
    std::string get_rel_path(const std::string &sub_path) const;
};

class http_server_base : public http_service
{
public:
    explicit http_server_base()
    {
        static std::once_flag flag;
        std::call_once(flag, [&]() {
            register_handler("updateConfig",
                             std::bind(&http_server_base::update_config_handler,
                                       this,
                                       std::placeholders::_1,
                                       std::placeholders::_2),
                             "<key>=<new_value>",
                             "Update the config to the new value.");
        });
    }

    std::string path() const override { return ""; }

protected:
    void update_config_handler(const http_request &req, http_response &resp);

    virtual void update_config(const std::string &name) {}
};

// Example:
//
// ```
// register_http_call("/meta/app")
//     .with_callback(std::bind(&meta_http_service::get_app_handler,
//                              this,
//                              std::placeholders::_1,
//                              std::placeholders::_2))
//     .with_help("Gets the app information.")
//     .add_argument("app_name", HTTP_ARG_STRING);
// ```
extern http_call &register_http_call(std::string full_path);

extern void deregister_http_call(const std::string &full_path);

// Starts serving HTTP requests.
// The internal HTTP server will reuse the rDSN server port.
extern void start_http_server();

// NOTE: the memory of `svc` will be transferred to the underlying registry.
// TODO(wutao): pass `svc` as a std::unique_ptr.
extern void register_http_service(http_service *svc);

inline bool is_http_message(dsn::task_code code)
{
    return code == RPC_HTTP_SERVICE || code == RPC_HTTP_SERVICE_ACK;
}

} // namespace dsn
