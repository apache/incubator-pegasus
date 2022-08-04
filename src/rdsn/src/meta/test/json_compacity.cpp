/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <gtest/gtest.h>

#include <dsn/service_api_c.h>
#include <dsn/service_api_cpp.h>

#include "meta/meta_service.h"
#include "meta/server_state.h"
#include "meta/meta_backup_service.h"
#include "meta_service_test_app.h"

namespace dsn {
namespace replication {

void meta_service_test_app::json_compacity()
{
    dsn::app_info info;
    info.app_id = 1;
    info.app_name = "test";
    info.app_type = "test";
    info.expire_second = 30;
    info.is_stateful = true;
    info.max_replica_count = 3;
    info.partition_count = 32;
    info.status = dsn::app_status::AS_AVAILABLE;

    dsn::app_info info2;

    // 1. encoded data can be decoded
    dsn::blob bb = dsn::json::json_forwarder<dsn::app_info>::encode(info);
    std::cout << bb.data() << std::endl;
    ASSERT_TRUE(dsn::json::json_forwarder<dsn::app_info>::decode(bb, info2));
    ASSERT_EQ(info2, info);

    // 2. old version of json can be decoded to new struct
    const char *json = "{\"status\":\"app_status::AS_AVAILABLE\","
                       "\"app_type\":\"pegasus\",\"app_name\":\"temp\","
                       "\"app_id\":1,\"partition_count\":16,\"envs\":{},"
                       "\"is_stateful\":1,\"max_replica_count\":3}";
    dsn::json::json_forwarder<dsn::app_info>::decode(dsn::blob(json, 0, strlen(json)), info2);
    ASSERT_EQ(info2.app_name, "temp");
    ASSERT_EQ(info2.max_replica_count, 3);

    // 3. older version
    info2 = info;
    const char *json2 = "{\"status\":\"app_status::AS_AVAILABLE\","
                        "\"app_type\":\"pegasus\",\"app_name\":\"temp\","
                        "\"app_id\":1,\"partition_count\":16,\"envs\":{}}";
    dsn::json::json_forwarder<dsn::app_info>::decode(dsn::blob(json2, 0, strlen(json2)), info2);
    ASSERT_EQ(info2.app_name, "temp");
    ASSERT_EQ(info2.app_type, "pegasus");
    ASSERT_EQ(info2.partition_count, 16);

    // 4. old pc version
    const char *json3 = "{\"pid\":\"1.1\",\"ballot\":234,\"max_replica_count\":3,"
                        "\"primary\":\"invalid address\",\"secondaries\":[\"127.0.0.1:6\"],"
                        "\"last_drops\":[],\"last_committed_decree\":157}";
    dsn::partition_configuration pc;
    dsn::json::json_forwarder<dsn::partition_configuration>::decode(
        dsn::blob(json3, 0, strlen(json3)), pc);
    ASSERT_EQ(234, pc.ballot);
    ASSERT_TRUE(pc.primary.is_invalid());
    ASSERT_EQ(1, pc.secondaries.size());
    ASSERT_EQ(0, strcmp(pc.secondaries[0].to_string(), "127.0.0.1:6"));
    ASSERT_EQ(157, pc.last_committed_decree);
    ASSERT_EQ(0, pc.partition_flags);

    // 5. not valid json
    const char *json4 = "{\"pid\":\"1.1\",\"ballot\":234,\"max_replica_count\":3,"
                        "\"primary\":\"invalid address\",\"secondaries\":[\"127.0.0.1:6\","
                        "\"last_drops\":[],\"last_committed_decree\":157}";
    dsn::blob in(json4, 0, strlen(json4));
    bool result = dsn::json::json_forwarder<dsn::partition_configuration>::decode(in, pc);
    ASSERT_FALSE(result);

    // 6 app_name with ':'
    const char *json6 = "{\"status\":\"app_status::AS_AVAILABLE\","
                        "\"app_type\":\"pegasus\",\"app_name\":\"CL769:test\","
                        "\"app_id\":1,\"partition_count\":16,\"envs\":{},"
                        "\"is_stateful\":1,\"max_replica_count\":3}";
    result =
        dsn::json::json_forwarder<dsn::app_info>::decode(dsn::blob(json6, 0, strlen(json6)), info2);
    ASSERT_TRUE(result);
    ASSERT_EQ(info2.app_name, "CL769:test");
    ASSERT_EQ(info2.max_replica_count, 3);
}

} // namespace replication
} // namespace dsn
