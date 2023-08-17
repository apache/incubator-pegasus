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
#include <functional>
#include <string>
#include <unordered_map>

#include "http/http_method.h"
#include "utils/errors.h"
#include "utils/ports.h"
#include "utils/string_view.h"

namespace dsn {

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
// const auto &err = client.init();
class http_client
{
public:
    using http_callback = std::function<bool(const void *data, size_t length)>;

    http_client();
    ~http_client();

    dsn::error_s init();

    dsn::error_s set_method(http_method method);
    dsn::error_s set_url(const std::string &url);
    dsn::error_s set_timeout(long timeout_ms);

    void clear_header_fields();
    void set_accept(dsn::string_view val);
    void set_content_type(dsn::string_view val);

    dsn::error_s do_method(const http_callback &callback = {});
    dsn::error_s do_method(std::string *response);

    dsn::error_s get_http_status(long &http_status) const;

private:
    using header_field_map = std::unordered_map<std::string, std::string>;

    void clear_error_buf();
    bool is_error_buf_empty() const;
    std::string to_error_msg(CURLcode code) const;
    size_t on_response_data(const void *data, size_t length);

    void free_header_list();
    void set_header_field(dsn::string_view key, dsn::string_view val);
    dsn::error_s process_header();

    // The size of a buffer that is used by libcurl to store human readable
    // error messages on failures or problems.
    static const constexpr size_t kErrorBufferBytes = CURL_ERROR_SIZE;

    CURL *_curl;
    http_method _method;
    std::string _url;
    const http_callback *_callback;
    char _error_buf[kErrorBufferBytes];

    bool _header_changed;
    header_field_map _header_fields;
    struct curl_slist *_header_list;

    DISALLOW_COPY_AND_ASSIGN(http_client);
};

} // namespace dsn
