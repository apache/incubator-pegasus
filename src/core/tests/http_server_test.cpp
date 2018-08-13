// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/tool-api/http_server.h>
#include <gtest/gtest.h>

namespace dsn {

TEST(http_server, parse_url)
{
    struct test_case
    {
        std::string url;

        error_code err;
        std::pair<std::string, std::string> result;
    } tests[] = {
        {"http://127.0.0.1:34601", ERR_OK, {"", ""}},
        {"http://127.0.0.1:34601/", ERR_OK, {"", ""}},
        {"http://127.0.0.1:34601///", ERR_OK, {"", ""}},
        {"http://127.0.0.1:34601/threads", ERR_OK, {"threads", ""}},
        {"http://127.0.0.1:34601/threads/", ERR_OK, {"threads", ""}},
        {"http://127.0.0.1:34601//pprof/heap/", ERR_OK, {"pprof", "heap"}},
        {"http://127.0.0.1:34601//pprof///heap", ERR_OK, {"pprof", "heap"}},
        {"http://127.0.0.1:34601/pprof/heap/arg/", ERR_INVALID_PARAMETERS, {}},
    };

    for (auto tt : tests) {
        ref_ptr<message_ex> m = message_ex::create_receive_message_with_standalone_header(
            blob::create_from_bytes(std::string("POST")));
        m->buffers.emplace_back(blob::create_from_bytes(std::string(tt.url)));

        auto res = http_request::parse(m.get());
        if (res.is_ok()) {
            ASSERT_EQ(res.get_value().service_method, tt.result) << tt.url;
        } else {
            ASSERT_EQ(res.get_error().code(), tt.err);
        }
    }
}

} // namespace dsn
