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

#include <string.h>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/json_helper.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "meta/meta_backup_service.h"
#include "meta_service_test_app.h"
#include "runtime/rpc/rpc_address.h"
#include "utils/blob.h"

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
    ASSERT_STREQ("127.0.0.1:6", pc.secondaries[0].to_string());
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

    // 7. policy can be decoded correctly
    const char *json7 = "{\"policy_name\":\"every_day\",\"backup_provider_type\":\"simple\",\"app_"
                        "ids\":[4,5,6,7,8,9,10,11,14,15,16,17,18,19,21,22,23,24],\"app_names\":{"
                        "\"4\":\"aaaa\",\"5\":\"aaaa\",\"6\":\"aaaa\",\"7\":\"aaaa\",\"8\":"
                        "\"aaaa\",\"9\":\"aaaa\",\"10\":\"aaaa\",\"11\":\"aaaa\",\"14\":\"aaaa\","
                        "\"15\":\"aaaa\",\"16\":\"aaaa\",\"17\":\"aaaa\",\"18\":\"aaaa\",\"19\":"
                        "\"aaaa\",\"21\":\"aaaa\",\"22\":\"aaaa\",\"23\":\"aaaa\",\"24\":\"aaaa\"},"
                        "\"backup_interval_seconds\":86400,\"backup_history_count_to_keep\":3,\"is_"
                        "disable\":0,\"start_time\":{\"hour\":0,\"minute\":30}}";
    dsn::replication::policy p;
    result = dsn::json::json_forwarder<dsn::replication::policy>::decode(
        dsn::blob(json7, 0, strlen(json7)), p);
    ASSERT_TRUE(result);
    ASSERT_EQ("every_day", p.policy_name);
    ASSERT_EQ("simple", p.backup_provider_type);

    std::set<int32_t> app_ids = {4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24};
    ASSERT_EQ(app_ids, p.app_ids);

    std::map<int32_t, std::string> app_names;
    for (int32_t i : app_ids) {
        app_names.emplace(i, "aaaa");
    }
    ASSERT_EQ(app_names, p.app_names);
    ASSERT_EQ(86400, p.backup_interval_seconds);
    ASSERT_EQ(3, p.backup_history_count_to_keep);
    ASSERT_EQ(0, p.is_disable);
    ASSERT_EQ(0, p.start_time.hour);
    ASSERT_EQ(30, p.start_time.minute);

    // 8. backup info can be decoded correctly
    const char *json8 =
        "{\"backup_id\":1528216470578,\"start_time_ms\":1528216470578,\"end_time_"
        "ms\":1528217091629,\"app_ids\":[4,5,6,7,8,9,10,11,14,15,16,17,18,19,21,22,"
        "23,24],\"app_names\":{\"4\":\"aaaa\",\"5\":\"aaaa\",\"6\":\"aaaa\",\"7\":\"aaaa\",\"8\":"
        "\"aaaa\",\"9\":\"aaaa\",\"10\":\"aaaa\",\"11\":\"aaaa\",\"14\":\"aaaa\",\"15\":\"aaaa\","
        "\"16\":"
        "\"aaaa\",\"17\":\"aaaa\",\"18\":\"aaaa\",\"19\":\"aaaa\",\"21\":\"aaaa\",\"22\":\"aaaa\","
        "\"23\":"
        "\"aaaa\",\"24\":\"aaaa\"},\"info_status\":1}";
    dsn::replication::backup_info binfo;
    result = dsn::json::json_forwarder<dsn::replication::backup_info>::decode(
        dsn::blob(json8, 0, strlen(json8)), binfo);
    ASSERT_TRUE(result);
    ASSERT_EQ(1528216470578, binfo.backup_id);
    ASSERT_EQ(1528216470578, binfo.start_time_ms);
    ASSERT_EQ(1528217091629, binfo.end_time_ms);
    ASSERT_EQ(app_ids, binfo.app_ids);
    ASSERT_EQ(app_names, binfo.app_names);
    ASSERT_EQ(1, binfo.info_status);
}

} // namespace replication
} // namespace dsn
