#include <gtest/gtest.h>

#include <dsn/service_api_c.h>
#include <dsn/service_api_cpp.h>

#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/server_state.h"
#include "meta_service_test_app.h"

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
    dsn::json::json_forwarder<dsn::app_info>::decode(bb, info2);
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
}
