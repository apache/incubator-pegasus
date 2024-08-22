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

#include "curl/curl.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"

DSN_DEFINE_uint32(http,
                  curl_timeout_ms,
                  10000,
                  "The maximum time in milliseconds that you allow the libcurl transfer operation "
                  "to complete");

namespace dsn {

#define RETURN_IF_CURL_NOT_OK(expr, ok, ...)                                                       \
    do {                                                                                           \
        const auto &code = (expr);                                                                 \
        if (dsn_unlikely(code != (ok))) {                                                          \
            std::string msg(fmt::format("{}: {}", fmt::format(__VA_ARGS__), to_error_msg(code)));  \
            return dsn::error_s::make(to_error_code(code), msg);                                   \
        }                                                                                          \
    } while (0)

#define CHECK_IF_CURL_OK(expr, ok, ...)                                                            \
    do {                                                                                           \
        const auto &code = (expr);                                                                 \
        CHECK_EXPRESSION(                                                                          \
            #expr == #ok, code == (ok), "{}: {}", fmt::format(__VA_ARGS__), to_error_msg(code));   \
    } while (0)

#define CHECK_IF_CURL_URL_OK(url, expr, ...)                                                       \
    do {                                                                                           \
        CHECK_NOTNULL(url, "CURLU object has not been allocated");                                 \
        CHECK_IF_CURL_OK(expr, CURLUE_OK, __VA_ARGS__);                                            \
    } while (0)

#define CHECK_IF_CURL_URL_SET_OK(url, part, content, ...)                                          \
    CHECK_IF_CURL_URL_OK(url, curl_url_set(url, CURLUPART_##part, content, 0), __VA_ARGS__)

#define CHECK_IF_CURL_URL_SET_NULL_OK(url, part, ...)                                              \
    CHECK_IF_CURL_URL_SET_OK(url,                                                                  \
                             part,                                                                 \
                             nullptr,                                                              \
                             "curl_url_set(part = " #part                                          \
                             ", content = nullptr) should always return CURLUE_OK")

// Set "http" as the default scheme.
#define SET_DEFAULT_HTTP_SCHEME(url)                                                               \
    CHECK_IF_CURL_URL_SET_OK(url,                                                                  \
                             SCHEME,                                                               \
                             enum_to_val(http_scheme::kHttp, std::string()).c_str(),               \
                             "failed to set CURLUPART_SCHEME with 'http'")

namespace {

inline dsn::error_code to_error_code(CURLUcode code)
{
    switch (code) {
    case CURLUE_OK:
        return dsn::ERR_OK;
    default:
        return dsn::ERR_CURL_FAILED;
    }
}

inline std::string to_error_msg(CURLUcode code)
{
    return fmt::format("code={}, desc=\"{}\"", static_cast<int>(code), curl_url_strerror(code));
}

CURLU *new_curlu()
{
    CURLU *url = curl_url();
    CHECK_NOTNULL(url, "fail to allocate a CURLU object due to out of memory");

    SET_DEFAULT_HTTP_SCHEME(url);

    return url;
}

CURLU *dup_curlu(const CURLU *url)
{
    CURLU *new_url = curl_url_dup(url);
    CHECK_NOTNULL(new_url, "fail to duplicate a CURLU object due to out of memory");

    return new_url;
}

} // anonymous namespace

void http_url::curlu_deleter::operator()(CURLU *url) const
{
    if (url == nullptr) {
        return;
    }

    curl_url_cleanup(url);
    url = nullptr;
}

http_url::http_url() noexcept : _url(new_curlu(), curlu_deleter()) {}

http_url::http_url(const http_url &rhs) noexcept : _url(dup_curlu(rhs._url.get()), curlu_deleter())
{
}

http_url &http_url::operator=(const http_url &rhs) noexcept
{
    if (this == &rhs) {
        return *this;
    }

    // The existing deleter will not be cleared.
    _url.reset(dup_curlu(rhs._url.get()));
    return *this;
}

http_url::http_url(http_url &&rhs) noexcept : _url(std::move(rhs._url)) {}

http_url &http_url::operator=(http_url &&rhs) noexcept
{
    if (this == &rhs) {
        return *this;
    }

    _url = std::move(rhs._url);
    return *this;
}

void http_url::clear()
{
    // Setting the url with nullptr would lead to the release of memory for each part of the url,
    // thus clearing the url.
    CHECK_IF_CURL_URL_SET_NULL_OK(_url.get(), URL);

    SET_DEFAULT_HTTP_SCHEME(_url.get());
}

#define RETURN_IF_CURL_URL_NOT_OK(expr, ...) RETURN_IF_CURL_NOT_OK(expr, CURLUE_OK, __VA_ARGS__)

#define RETURN_IF_CURL_URL_SET_NOT_OK(part, content, flags)                                        \
    RETURN_IF_CURL_URL_NOT_OK(                                                                     \
        curl_url_set(_url.get(), part, content, flags), "failed to set " #part " to {}", content)

#define RETURN_IF_CURL_URL_GET_NOT_OK(part, content_pointer, flags)                                \
    RETURN_IF_CURL_URL_NOT_OK(curl_url_get(_url.get(), part, content_pointer, flags),              \
                              "failed to get " #part)

#define DEF_HTTP_URL_SET_FUNC(name, part)                                                          \
    dsn::error_s http_url::set_##name(const char *content)                                         \
    {                                                                                              \
        RETURN_IF_CURL_URL_SET_NOT_OK(CURLUPART_##part, content, 0);                               \
                                                                                                   \
        return dsn::error_s::ok();                                                                 \
    }

DEF_HTTP_URL_SET_FUNC(url, URL)

DEF_HTTP_URL_SET_FUNC(scheme, SCHEME)

dsn::error_s http_url::set_scheme(http_scheme scheme)
{
    return set_scheme(enum_to_val(scheme, std::string()).c_str());
}

DEF_HTTP_URL_SET_FUNC(host, HOST)

DEF_HTTP_URL_SET_FUNC(port, PORT)

dsn::error_s http_url::set_port(uint16_t port) { return set_port(std::to_string(port).c_str()); }

DEF_HTTP_URL_SET_FUNC(path, PATH)

DEF_HTTP_URL_SET_FUNC(query, QUERY)

#undef DEF_HTTP_URL_SET_FUNC

dsn::error_s http_url::to_string(std::string &url) const
{
    CHECK_NOTNULL(_url, "CURLU object has not been allocated");

    char *content;
    RETURN_IF_CURL_URL_GET_NOT_OK(CURLUPART_URL, &content, 0);

    url = content;

    // Free the returned string.
    curl_free(content);

    return dsn::error_s::ok();
}

#undef RETURN_IF_CURL_URL_GET_NOT_OK
#undef RETURN_IF_CURL_URL_SET_NOT_OK
#undef RETURN_IF_CURL_URL_NOT_OK

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

#define RETURN_IF_CURL_EASY_NOT_OK(expr, ...)                                                      \
    do {                                                                                           \
        CHECK_NOTNULL(_curl, "CURL object has not been allocated");                                \
        RETURN_IF_CURL_NOT_OK(expr, CURLE_OK, __VA_ARGS__);                                        \
    } while (0)

#define RETURN_IF_CURL_EASY_SETOPT_NOT_OK(opt, input)                                              \
    RETURN_IF_CURL_EASY_NOT_OK(curl_easy_setopt(_curl, CURLOPT_##opt, input),                      \
                               "failed to set " #opt " with " #input)

#define RETURN_IF_CURL_EASY_GETINFO_NOT_OK(info, output)                                           \
    RETURN_IF_CURL_EASY_NOT_OK(curl_easy_getinfo(_curl, CURLINFO_##info, output),                  \
                               "failed to get from " #info)

#define RETURN_IF_CURL_EASY_PERFORM_NOT_OK()                                                       \
    RETURN_IF_CURL_EASY_NOT_OK(curl_easy_perform(_curl),                                           \
                               "failed to perform http request(method={}, url={})",                \
                               enum_to_string(_method),                                            \
                               _url)

#define CHECK_IF_CURL_EASY_OK(expr, ...)                                                           \
    do {                                                                                           \
        CHECK_NOTNULL(_curl, "CURL object has not been allocated");                                \
        CHECK_IF_CURL_OK(expr, CURLE_OK, __VA_ARGS__);                                             \
    } while (0)

#define CHECK_IF_CURL_EASY_SETOPT_OK(opt, input, ...)                                              \
    CHECK_IF_CURL_EASY_OK(curl_easy_setopt(_curl, CURLOPT_##opt, input),                           \
                          "failed to set " #opt " with " #input ": {}",                            \
                          fmt::format(__VA_ARGS__))

dsn::error_s http_client::init()
{
    if (_curl == nullptr) {
        _curl = curl_easy_init();
        if (_curl == nullptr) {
            return dsn::error_s::make(dsn::ERR_CURL_FAILED, "fail to allocate CURL object");
        }
    } else {
        curl_easy_reset(_curl);
    }

    clear_header_fields();
    free_header_list();

    // Additional messages for errors are needed.
    clear_error_buf();
    RETURN_IF_CURL_EASY_SETOPT_NOT_OK(ERRORBUFFER, _error_buf);

    // Set with NOSIGNAL since we are multi-threaded.
    RETURN_IF_CURL_EASY_SETOPT_NOT_OK(NOSIGNAL, 1L);

    // Redirects are supported.
    RETURN_IF_CURL_EASY_SETOPT_NOT_OK(FOLLOWLOCATION, 1L);

    // Before 8.3.0, CURLOPT_MAXREDIRS was unlimited.
    RETURN_IF_CURL_EASY_SETOPT_NOT_OK(MAXREDIRS, 20);

    // Set common timeout for transfer operation. Users could also change it with their
    // custom values by `set_timeout`.
    RETURN_IF_CURL_EASY_SETOPT_NOT_OK(TIMEOUT_MS, static_cast<long>(FLAGS_curl_timeout_ms));

    // A lambda can only be converted to a function pointer if it does not capture:
    // https://stackoverflow.com/questions/28746744/passing-capturing-lambda-as-function-pointer
    // http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2012/n3337.pdf
    curl_write_callback callback = [](char *buffer, size_t size, size_t nmemb, void *param) {
        http_client *client = reinterpret_cast<http_client *>(param);
        return client->on_response_data(buffer, size * nmemb);
    };
    RETURN_IF_CURL_EASY_SETOPT_NOT_OK(WRITEFUNCTION, callback);

    // This http_client object itself is passed to the callback function.
    RETURN_IF_CURL_EASY_SETOPT_NOT_OK(WRITEDATA, reinterpret_cast<void *>(this));

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

// Once CURLOPT_CURLU is supported, actually curl_easy_setopt would always return CURLE_OK,
// see https://curl.se/libcurl/c/CURLOPT_CURLU.html"). For the reason why we still return
// ERR_OK, please see the comments for set_url.
#define RETURN_IF_CURL_EASY_SETOPT_CURLU_NOT_OK()                                                  \
    RETURN_IF_CURL_EASY_SETOPT_NOT_OK(CURLU, _url.curlu())

dsn::error_s http_client::set_url(const std::string &new_url)
{
    // DO NOT call curl_easy_setopt() on CURLOPT_URL, since the CURLOPT_URL string is ignored
    // if CURLOPT_CURLU is set. See following docs for details:
    // * https://curl.se/libcurl/c/CURLOPT_CURLU.html
    // * https://curl.se/libcurl/c/CURLOPT_URL.html
    //
    // Use a temporary object for the reason that once the error occurred, `_url` would not
    // be dirty.
    http_url tmp;
    RETURN_NOT_OK(tmp.set_url(new_url.c_str()));

    RETURN_NOT_OK(set_url(std::move(tmp)));
    return dsn::error_s::ok();
}

dsn::error_s http_client::set_url(const http_url &new_url)
{
    _url = new_url;
    RETURN_IF_CURL_EASY_SETOPT_CURLU_NOT_OK();
    return dsn::error_s::ok();
}

dsn::error_s http_client::set_url(http_url &&new_url)
{
    _url = std::move(new_url);
    RETURN_IF_CURL_EASY_SETOPT_CURLU_NOT_OK();
    return dsn::error_s::ok();
}

dsn::error_s http_client::with_post_method(const std::string &data)
{
    // No need to enable CURLOPT_POST by `RETURN_IF_CURL_EASY_SETOPT_NOT_OK(POST, 1L)`,
    // since using either of CURLOPT_POSTFIELDS or CURLOPT_COPYPOSTFIELDS implies setting
    // CURLOPT_POST to 1.
    //
    // See https://curl.se/libcurl/c/CURLOPT_POSTFIELDS.html for details.
    RETURN_IF_CURL_EASY_SETOPT_NOT_OK(POSTFIELDSIZE, static_cast<long>(data.size()));
    RETURN_IF_CURL_EASY_SETOPT_NOT_OK(COPYPOSTFIELDS, data.data());
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
        RETURN_IF_CURL_EASY_SETOPT_NOT_OK(HTTPGET, 1L);
        break;
    default:
        LOG_FATAL("Unsupported http_method");
    }

    _method = method;
    return dsn::error_s::ok();
}

dsn::error_s http_client::set_auth(http_auth_type auth_type)
{
    switch (auth_type) {
    case http_auth_type::SPNEGO:
        RETURN_IF_CURL_EASY_SETOPT_NOT_OK(HTTPAUTH, CURLAUTH_NEGOTIATE);
        break;
    case http_auth_type::DIGEST:
        RETURN_IF_CURL_EASY_SETOPT_NOT_OK(HTTPAUTH, CURLAUTH_DIGEST);
        break;
    case http_auth_type::BASIC:
        RETURN_IF_CURL_EASY_SETOPT_NOT_OK(HTTPAUTH, CURLAUTH_BASIC);
        break;
    case http_auth_type::NONE:
        break;
    default:
        RETURN_IF_CURL_EASY_SETOPT_NOT_OK(HTTPAUTH, CURLAUTH_ANY);
        break;
    }

    return dsn::error_s::ok();
}

dsn::error_s http_client::set_timeout(long timeout_ms)
{
    RETURN_IF_CURL_EASY_SETOPT_NOT_OK(TIMEOUT_MS, timeout_ms);
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

void http_client::set_header_field(std::string_view key, std::string_view val)
{
    _header_fields[std::string(key)] = std::string(val);
    _header_changed = true;
}

void http_client::set_accept(std::string_view val) { set_header_field("Accept", val); }

void http_client::set_content_type(std::string_view val) { set_header_field("Content-Type", val); }

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
    RETURN_IF_CURL_EASY_SETOPT_NOT_OK(HTTPHEADER, _header_list);

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

    RETURN_IF_CURL_EASY_PERFORM_NOT_OK();
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

dsn::error_s http_client::get_http_status(http_status_code &status_code) const
{
    long response_code;
    RETURN_IF_CURL_EASY_GETINFO_NOT_OK(RESPONSE_CODE, &response_code);
    status_code = enum_from_val(response_code, http_status_code::kInvalidCode);
    return dsn::error_s::ok();
}

#undef RETURN_IF_CURL_EASY_PERFORM_NOT_OK
#undef RETURN_IF_CURL_EASY_GETINFO_NOT_OK
#undef RETURN_IF_CURL_EASY_SETOPT_NOT_OK
#undef RETURN_IF_CURL_EASY_NOT_OK

#undef RETURN_IF_CURL_NOT_OK

} // namespace dsn
