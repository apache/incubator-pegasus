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
#include <utility>

#include "curl/curl.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"

namespace dsn {

DSN_DEFINE_uint32(http,
                  curl_timeout_ms,
                  10000,
                  "The maximum time in milliseconds that you allow the libcurl transfer operation "
                  "to complete");

http_client::http_client()
    : _curl(nullptr),
      _method(http_method::GET),
      _recv_callback(nullptr),
      _header_changed(true),
      _header_list(nullptr)
{
    // Since `kErrorBufferBytes` is private, `static_assert` have to be put in constructor.
    static_assert(http_client::kErrorBufferBytes >= CURL_ERROR_SIZE,
                  "The error buffer used by libcurl must be at least CURL_ERROR_SIZE bytes big");

    clear_error_buf();
}

http_client::~http_client()
{
    if (_curl != nullptr) {
        curl_easy_cleanup(_curl);
        _curl = nullptr;
    }

    free_header_list();
}

namespace {

inline dsn::error_code to_error_code(CURLcode code)
{
    switch (code) {
    case CURLE_OK:
        return dsn::ERR_OK;
    case CURLE_OPERATION_TIMEDOUT:
        return dsn::ERR_TIMEOUT;
    default:
        return dsn::ERR_CURL_FAILED;
    }
}

} // anonymous namespace

#define RETURN_IF_CURL_NOT_OK(expr, ...)                                                           \
    do {                                                                                           \
        const auto code = (expr);                                                                  \
        if (dsn_unlikely(code != CURLE_OK)) {                                                      \
            std::string msg(fmt::format("{}: {}", fmt::format(__VA_ARGS__), to_error_msg(code)));  \
            return dsn::error_s::make(to_error_code(code), msg);                                   \
        }                                                                                          \
    } while (0)

#define RETURN_IF_SETOPT_NOT_OK(opt, input)                                                        \
    RETURN_IF_CURL_NOT_OK(curl_easy_setopt(_curl, opt, input),                                     \
                          "failed to set " #opt " to"                                              \
                          " " #input)

#define RETURN_IF_GETINFO_NOT_OK(info, output)                                                     \
    RETURN_IF_CURL_NOT_OK(curl_easy_getinfo(_curl, info, output), "failed to get from " #info)

#define RETURN_IF_EXEC_METHOD_NOT_OK()                                                             \
    RETURN_IF_CURL_NOT_OK(curl_easy_perform(_curl),                                                \
                          "failed to perform http request(method={}, url={})",                     \
                          enum_to_string(_method),                                                 \
                          _url)

dsn::error_s http_client::init()
{
    if (_curl == nullptr) {
        _curl = curl_easy_init();
        if (_curl == nullptr) {
            return dsn::error_s::make(dsn::ERR_CURL_FAILED, "fail to initialize curl");
        }
    } else {
        curl_easy_reset(_curl);
    }

    clear_header_fields();
    free_header_list();

    // Additional messages for errors are needed.
    clear_error_buf();
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_ERRORBUFFER, _error_buf);

    // Set with NOSIGNAL since we are multi-threaded.
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_NOSIGNAL, 1L);

    // Redirects are supported.
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_FOLLOWLOCATION, 1L);

    // Before 8.3.0, CURLOPT_MAXREDIRS was unlimited.
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_MAXREDIRS, 20);

    // Set common timeout for transfer operation. Users could also change it with their
    // custom values by `set_timeout`.
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_TIMEOUT_MS, static_cast<long>(FLAGS_curl_timeout_ms));

    // A lambda can only be converted to a function pointer if it does not capture:
    // https://stackoverflow.com/questions/28746744/passing-capturing-lambda-as-function-pointer
    // http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2012/n3337.pdf
    curl_write_callback callback = [](char *buffer, size_t size, size_t nmemb, void *param) {
        http_client *client = reinterpret_cast<http_client *>(param);
        return client->on_response_data(buffer, size * nmemb);
    };
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_WRITEFUNCTION, callback);

    // This http_client object itself is passed to the callback function.
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_WRITEDATA, reinterpret_cast<void *>(this));

    return dsn::error_s::ok();
}

void http_client::clear_error_buf() { _error_buf[0] = 0; }

bool http_client::is_error_buf_empty() const { return _error_buf[0] == 0; }

std::string http_client::to_error_msg(CURLcode code) const
{
    std::string err_msg =
        fmt::format("code={}, desc=\"{}\"", static_cast<int>(code), curl_easy_strerror(code));
    if (is_error_buf_empty()) {
        return err_msg;
    }

    err_msg += fmt::format(", msg=\"{}\"", _error_buf);
    return err_msg;
}

// `data` passed to this function is NOT null-terminated.
// `length` might be zero.
size_t http_client::on_response_data(const void *data, size_t length)
{
    if (_recv_callback == nullptr) {
        return length;
    }

    if (!(*_recv_callback)) {
        // callback function is empty.
        return length;
    }

    // According to libcurl, callback should return the number of bytes actually taken care of.
    // If that amount differs from the amount passed to callback function, it would signals an
    // error condition. This causes the transfer to get aborted and the libcurl function used
    // returns CURLE_WRITE_ERROR. Therefore, here we just return the max limit of size_t for
    // failure.
    //
    // See https://curl.se/libcurl/c/CURLOPT_WRITEFUNCTION.html for details.
    return (*_recv_callback)(data, length) ? length : std::numeric_limits<size_t>::max();
}

dsn::error_s http_client::set_url(const std::string &url)
{
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_URL, url.c_str());

    _url = url;
    return dsn::error_s::ok();
}

dsn::error_s http_client::with_post_method(const std::string &data)
{
    // No need to enable CURLOPT_POST by `RETURN_IF_SETOPT_NOT_OK(CURLOPT_POST, 1L)`, since using
    // either of CURLOPT_POSTFIELDS or CURLOPT_COPYPOSTFIELDS implies setting CURLOPT_POST to 1.
    // See https://curl.se/libcurl/c/CURLOPT_POSTFIELDS.html for details.
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_POSTFIELDSIZE, static_cast<long>(data.size()));
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_COPYPOSTFIELDS, data.data());
    _method = http_method::POST;
    return dsn::error_s::ok();
}

dsn::error_s http_client::with_get_method() { return set_method(http_method::GET); }

dsn::error_s http_client::set_method(http_method method)
{
    // No need to process the case of http_method::POST, since it should be enabled by
    // `with_post_method`.
    switch (method) {
    case http_method::GET:
        RETURN_IF_SETOPT_NOT_OK(CURLOPT_HTTPGET, 1L);
        break;
    default:
        LOG_FATAL("Unsupported http_method");
    }

    _method = method;
    return dsn::error_s::ok();
}

dsn::error_s http_client::set_auth(http_auth_type authType)
{
    switch (authType) {
    case http_auth_type::SPNEGO:
        RETURN_IF_SETOPT_NOT_OK(CURLOPT_HTTPAUTH, CURLAUTH_NEGOTIATE);
        break;
    case http_auth_type::DIGEST:
        RETURN_IF_SETOPT_NOT_OK(CURLOPT_HTTPAUTH, CURLAUTH_DIGEST);
        break;
    case http_auth_type::BASIC:
        RETURN_IF_SETOPT_NOT_OK(CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
        break;
    case http_auth_type::NONE:
        break;
    default:
        RETURN_IF_SETOPT_NOT_OK(CURLOPT_HTTPAUTH, CURLAUTH_ANY);
        break;
    }

    return dsn::error_s::ok();
}

dsn::error_s http_client::set_timeout(long timeout_ms)
{
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_TIMEOUT_MS, timeout_ms);
    return dsn::error_s::ok();
}

void http_client::clear_header_fields()
{
    _header_fields.clear();

    _header_changed = true;
}

void http_client::free_header_list()
{
    if (_header_list == nullptr) {
        return;
    }

    curl_slist_free_all(_header_list);
    _header_list = nullptr;
}

void http_client::set_header_field(absl::string_view key, absl::string_view val)
{
    _header_fields[std::string(key)] = std::string(val);
    _header_changed = true;
}

void http_client::set_accept(absl::string_view val) { set_header_field("Accept", val); }

void http_client::set_content_type(absl::string_view val) { set_header_field("Content-Type", val); }

dsn::error_s http_client::process_header()
{
    if (!_header_changed) {
        return dsn::error_s::ok();
    }

    free_header_list();

    for (const auto &field : _header_fields) {
        auto str = fmt::format("{}: {}", field.first, field.second);

        // A null pointer is returned if anything went wrong, otherwise the new list pointer is
        // returned. To avoid overwriting an existing non-empty list on failure, the new list
        // should be returned to a temporary variable which can be tested for NULL before updating
        // the original list pointer. (https://curl.se/libcurl/c/curl_slist_append.html)
        struct curl_slist *temp = curl_slist_append(_header_list, str.c_str());
        if (temp == nullptr) {
            free_header_list();
            return dsn::error_s::make(dsn::ERR_CURL_FAILED, "curl_slist_append failed");
        }
        _header_list = temp;
    }

    // This would work well even if `_header_list` is NULL pointer. Pass a NULL to this option
    // to reset back to no custom headers. (https://curl.se/libcurl/c/CURLOPT_HTTPHEADER.html)
    RETURN_IF_SETOPT_NOT_OK(CURLOPT_HTTPHEADER, _header_list);

    // New header has been built successfully, thus mark it unchanged.
    _header_changed = false;

    return dsn::error_s::ok();
}

dsn::error_s http_client::exec_method(const http_client::recv_callback &callback)
{
    // `curl_easy_perform` would run synchronously, thus it is safe to use the pointer to
    // `callback`.
    _recv_callback = &callback;

    RETURN_NOT_OK(process_header());

    RETURN_IF_EXEC_METHOD_NOT_OK();
    return dsn::error_s::ok();
}

dsn::error_s http_client::exec_method(std::string *response)
{
    if (response == nullptr) {
        return exec_method();
    }

    auto callback = [response](const void *data, size_t length) {
        response->append(reinterpret_cast<const char *>(data), length);
        return true;
    };

    return exec_method(callback);
}

dsn::error_s http_client::get_http_status(long &http_status) const
{
    RETURN_IF_GETINFO_NOT_OK(CURLINFO_RESPONSE_CODE, &http_status);
    return dsn::error_s::ok();
}

#undef RETURN_IF_EXEC_METHOD_NOT_OK
#undef RETURN_IF_SETOPT_NOT_OK
#undef RETURN_IF_CURL_NOT_OK

} // namespace dsn
