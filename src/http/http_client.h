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
#include <functional>
#include <string>

#include "http/http_method.h"
#include "utils/errors.h"

namespace pegasus {

// Not thread-safe.
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

    dsn::error_s do_method(long &http_status, const http_callback &callback = {});
    dsn::error_s do_method(long &http_status, std::string *response);

    dsn::error_s get_http_status(long &http_status) const;

private:
    void clear_error_buffer();
    bool is_error_buffer_empty() const;
    std::string to_error_msg(CURLcode code) const;
    size_t on_response_data(const void *data, size_t length);

    // The size of a buffer that is used by libcurl to store human readable
    // error messages on failures or problems.
    static const constexpr size_t kErrorBufferBytes = CURL_ERROR_SIZE;

    CURL *_curl;
    http_method _method;
    std::string _url;
    const http_callback *_callback;
    char _error_buf[kErrorBufferBytes];

    DISALLOW_COPY_AND_ASSIGN(http_client);
};

} // namespace pegasus
