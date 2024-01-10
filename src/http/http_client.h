// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <curl/curl.h>
#include <stddef.h>
#include <stdint.h>
#include <functional>
#include <string>
#include <unordered_map>

#include "absl/strings/string_view.h"
#include "http/http_method.h"
#include "http/http_status_code.h"
#include "utils/errors.h"
#include "utils/fmt_utils.h"
#include "utils/ports.h"

namespace dsn {

// A class that help CURLU object
// https://curl.se/libcurl/c/libcurl-url.html
class http_url
{
public:
    http_url();
    ~http_url();
    http_url(http_url &&);
    http_url &operator=(http_url &&);

    dsn::error_s init();
    dsn::error_s set_url(const char *url);
    dsn::error_s set_scheme(const char *scheme);
    dsn::error_s set_host(const char *host);
    dsn::error_s set_port(const char *port);
    dsn::error_s set_port(uint16_t port);
    dsn::error_s set_path(const char *path);
    dsn::error_s set_query(const char *query);

    dsn::error_s to_string(std::string &url) const;

    friend std::ostream &operator<<(std::ostream &os, const http_url &url)
    {
        std::string str;
        const auto &err = url.to_string(str);
        if (dsn_unlikely(!err)) {
            return os << err;
        }
        return os << str;
    }

private:
    friend class http_client;

    void free_curlu_object();

    CURLU *get_curlu_object() const;

    std::string to_error_msg(CURLUcode code) const;

    CURLU *_url;

    DISALLOW_COPY_AND_ASSIGN(http_url);
};

// A library for http client that provides convenient APIs to access http services, implemented
// based on libcurl (https://curl.se/libcurl/c/).
//
// This class is not thread-safe. Thus maintain one instance for each thread.
//
// Example of submitting GET request to remote http service
// --------------------------------------------------------
// Create an instance of http_client:
// http_client client;
//
// It's necessary to initialize the new instance before coming into use:
// auto err = client.init();
//
// Specify the target url that you would request for:
// err = client.set_url("http://<ip>:<port>/your/path");
//
// If you would use GET method, call `with_get_method`:
// err = client.with_get_method();
//
// If you would use POST method, call `with_post_method` with post data:
// err = client.with_post_method(post_data);
//
// Submit the request to remote http service:
// err = client.exec_method();
//
// If response data should be processed, use callback function:
// auto callback = [...](const void *data, size_t length) {
//     ......
//     return true;
// };
// err = client.exec_method(callback);
//
// Or just provide a string pointer:
// std::string response;
// err = client.exec_method(&response);
//
// Get the http status code after requesting:
// http_status_code status_code;
// err = client.get_http_status(status_code);
class http_client
{
public:
    using recv_callback = std::function<bool(const void *data, size_t length)>;

    http_client();
    ~http_client();

    // Before coming into use, init() must be called to initialize http client. It could also be
    // called to reset the http clients that have been initialized previously.
    dsn::error_s init();

    // Specify the target url that the request would be sent for.
    dsn::error_s set_url(const std::string &new_url);

    // Specify the target url by `http_url` class.
    dsn::error_s set_url(const http_url &new_url);
    dsn::error_s set_url(http_url &&new_url);

    // Using post method, with `data` as the payload for post body.
    dsn::error_s with_post_method(const std::string &data);

    // Using get method.
    dsn::error_s with_get_method();

    // Specify the maximum time in milliseconds that a request is allowed to complete.
    dsn::error_s set_timeout(long timeout_ms);

    // Specify the http auth type which include NONE BASIC DIGEST SPNEGO
    dsn::error_s set_auth(http_auth_type authType);

    // Operations for the header fields.
    void clear_header_fields();
    void set_accept(absl::string_view val);
    void set_content_type(absl::string_view val);

    // Submit request to remote http service, with response processed by callback function.
    //
    // `callback` function gets called by libcurl as soon as there is data received that needs
    // to be saved. For most transfers, this callback gets called many times and each invoke
    // delivers another chunk of data.
    //
    // This function would run synchronously, which means it would wait until the response was
    // returned and processed appropriately.
    dsn::error_s exec_method(const recv_callback &callback = {});

    // Submit request to remote http service, with response data returned in a string.
    //
    // This function would run synchronously, which means it would wait until the response was
    // returned and processed appropriately.
    dsn::error_s exec_method(std::string *response);

    // Get the last http status code after requesting.
    dsn::error_s get_http_status(http_status_code &status_code) const;

private:
    using header_field_map = std::unordered_map<std::string, std::string>;

    void clear_error_buf();
    bool is_error_buf_empty() const;
    std::string to_error_msg(CURLcode code) const;

    size_t on_response_data(const void *data, size_t length);

    // Specify which http method would be used, such as GET. Enabling POST should not use this
    // function (use `with_post_method` instead).
    dsn::error_s set_method(http_method method);

    void free_header_list();
    void set_header_field(absl::string_view key, absl::string_view val);
    dsn::error_s process_header();

    // The size of a buffer that is used by libcurl to store human readable
    // error messages on failures or problems.
    static const constexpr size_t kErrorBufferBytes = CURL_ERROR_SIZE;

    CURL *_curl;
    http_method _method;
    http_url _url;
    const recv_callback *_recv_callback;
    char _error_buf[kErrorBufferBytes];

    bool _header_changed;
    header_field_map _header_fields;
    struct curl_slist *_header_list;

    DISALLOW_COPY_AND_ASSIGN(http_client);
};

} // namespace dsn

USER_DEFINED_STRUCTURE_FORMATTER(::dsn::http_url);
