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

#include "http/http_client.h"

#include <fmt/core.h>
#include <limits>
#include "utils/fmt_logging.h"
#include "utils/ports.h"

namespace pegasus {

DSN_DEFINE_uint32(http,
                  libcurl_error_buffer_bytes,
                  CURL_ERROR_SIZE,
                  "The size of a buffer that is used by libcurl to store human readable "
                  "error messages on failures or problems");
DSN_DEFINE_validator(libcurl_error_buffer_bytes, [](uint32_t value) -> bool { return value >= CURL_ERROR_SIZE; });

static_assert(http_client::kErrorBufferBytes >= CURL_ERROR_SIZE, "The error buffer used by libcurl must be at least CURL_ERROR_SIZE bytes big");

http_client::http_client()
{

}

http_client::~http_client()
{
    if (_curl != nullptr) {
        curl_easy_cleanup(_curl);
        _curl = nullptr;
    }
}

            // TODO: err
#define RETURN_IF_CURL_NOT_OK(expr, err, ...) \
    do { \
    const auto code = (expr); \
    if (dsn_unlikely(code != CURLE_OK)) { \
        std::string msg(fmt::format("{}: {}", fmt::format(__VA_ARGS__), to_error_msg(code))); \
        return dsn::error_s::make(err, msg); \
    } \
    } while (0)

            // TODO: err
#define RETURN_IF_SETOPT_NOT_OK(opt, param, err) \
    RETURN_IF_CURL_NOT_OK(curl_easy_setopt(_curl, opt, param), err, "failed to set " #opt " to " #param)

#define RETURN_IF_DO_METHOD_NOT_OK(err) \
    RETURN_IF_CURL_NOT_OK(curl_easy_perform(_curl), err, "failed to perform [method={}, url={}]", enum_to_string(_method), _url, action)

dsn::error_s http_client::init()
{
    if (_curl == nullptr) {
        _curl = curl_easy_init();
        if (_curl == nullptr) {
            // TODO: err
            return dsn::error_s::make(err, "fail to initialize curl");
        }
    } else {
        curl_easy_reset(_curl);
    }

    // Additional messages for errors are needed.
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_ERRORBUFFER, _error_buf);

    // Set with NOSIGNAL since we are multi-threaded.
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_NOSIGNAL, 1L);

    // Redirects are supported.
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_FOLLOWLOCATION, 1L);

    // Before 8.3.0, it was unlimited.
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_MAXREDIRS, 20);

    // A lambda can only be converted to a function pointer if it does not capture:
    // https://stackoverflow.com/questions/28746744/passing-capturing-lambda-as-function-pointer
    // http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2012/n3337.pdf
    curl_write_callback callback = [](char* buffer, size_t size, size_t nmemb, void* param) {
        http_client* client = reinterpret_cast<http_client*>(param);
        return client->on_response_data(buffer, size * nmemb);
    };
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_WRITEFUNCTION, callback);

    // This http_client object itself is passed to the callback function.
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_WRITEDATA, reinterpret_cast<void*>(this));

    return error_s::ok();
}

void http_client::clear_error_buf()
{
    _error_buf[0] = 0;
}

bool http_client::is_error_buf_empty()
{
    return _error_buf[0] == 0;
}

std::string http_client::to_error_msg(CURLcode code)
{
        std::string err_msg = fmt::format("code={}", curl_easy_strerror(code)); 
    if (is_error_buf_empty()) { 
        return err_msg;
    } 

    err_msg += fmt::format(", msg={}", _error_buf);
    return err_msg;
}

size_t http_client::on_response_data(const void* data, size_t length) {
    if (_callback == nullptr) {
        return length;
    }

    if (!(*_callback)) {
        return length;
    }

    return (*_callback)(data, length) ? length : std::numeric_limits<size_t>::max();
}

    // Error buffer should be cleared
    clear_error_buf();

dsn::error_s http_client::set_method(http_method method) {
    switch (method) {
    case http_method::GET:
        RETURN_IF_SETOPT_NOT_OK(CURLOPT_HTTPGET, 1L);
        break;
    case http_method::POST:
        RETURN_IF_SETOPT_NOT_OK(CURLOPT_POST, 1L);
        break;
    default:
        LOG_FATAL("Unsupported http_method");
    }

    _method = method;
    return dsn::error_s::ok();
}

dsn::error_s http_client::set_url(const std::string& url) {
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_URL, url.c_str());
    _url = url;
}

dsn::error_s http_client::do_method(const http_client::http_callback& callback) {
    _callback = &callback;
    RETURN_IF_DO_METHOD_NOT_OK();
    return dsn::error_s::ok();
}

dsn::error_s http_client::do_method(std::string* response) {
    if (response == nullptr) {
        return do_method();
    }

    auto callback = [response](const void* data, size_t length) {
        response->append(reinterpret_cast<char*>data, length);
        return true;
    };

    return do_method(callback);
}

#undef RETURN_IF_DO_METHOD_NOT_OK
#undef RETURN_IF_SETOPT_NOT_OK

} // namespace pegasus
