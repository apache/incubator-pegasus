/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "utils/time_utils.h"

#include "pegasus_server_test_base.h"
#include "server/pegasus_manual_compact_service.h"

namespace pegasus {
namespace server {

class manual_compact_service_test : public pegasus_server_test_base
{
public:
    std::unique_ptr<pegasus_manual_compact_service> manual_compact_svc;
    static const uint64_t compacted_ts = 1500000000; // 2017.07.14 10:40:00 CST

public:
    manual_compact_service_test()
    {
        start();
        manual_compact_svc = dsn::make_unique<pegasus_manual_compact_service>(_server.get());
    }

    void set_compact_time(int64_t ts)
    {
        manual_compact_svc->_manual_compact_last_finish_time_ms.store(
            static_cast<uint64_t>(ts * 1000));
    }

    void set_mock_now(uint64_t mock_now_sec)
    {
        manual_compact_svc->_mock_now_timestamp = mock_now_sec * 1000;
    }

    void check_compact_disabled(const std::map<std::string, std::string> &envs, bool ok)
    {
        ASSERT_EQ(ok, manual_compact_svc->check_compact_disabled(envs))
            << dsn::utils::kv_map_to_string(envs, ';', '=');
    }

    void check_once_compact(const std::map<std::string, std::string> &envs, bool ok)
    {
        ASSERT_EQ(ok, manual_compact_svc->check_once_compact(envs))
            << dsn::utils::kv_map_to_string(envs, ';', '=');
    }

    void check_periodic_compact(const std::map<std::string, std::string> &envs, bool ok)
    {
        ASSERT_EQ(ok, manual_compact_svc->check_periodic_compact(envs))
            << dsn::utils::kv_map_to_string(envs, ';', '=');
    }

    void extract_manual_compact_opts(const std::map<std::string, std::string> &envs,
                                     const std::string &key_prefix,
                                     rocksdb::CompactRangeOptions &options)
    {
        manual_compact_svc->extract_manual_compact_opts(envs, key_prefix, options);
    }

    void set_num_level(int level) { _server->_data_cf_opts.num_levels = level; }

    void check_manual_compact_state(bool ok, const std::string &msg = "")
    {
        ASSERT_EQ(ok, manual_compact_svc->check_manual_compact_state()) << msg;
    }

    void manual_compact(uint64_t mock_now_sec, uint64_t time_cost_sec)
    {
        set_mock_now(mock_now_sec);
        uint64_t start = manual_compact_svc->now_timestamp();
        manual_compact_svc->_manual_compact_start_running_time_ms.store(start);
        // do compacting...
        set_mock_now(mock_now_sec + time_cost_sec);
        uint64_t finish = manual_compact_svc->now_timestamp();
        manual_compact_svc->_manual_compact_last_finish_time_ms.store(finish);
        manual_compact_svc->_manual_compact_last_time_used_ms.store(finish - start);
        manual_compact_svc->_manual_compact_enqueue_time_ms.store(0);
    }

    void set_manual_compact_interval(int sec)
    {
        manual_compact_svc->_manual_compact_min_interval_seconds = sec;
    }
};

TEST_F(manual_compact_service_test, check_compact_disabled)
{
    std::map<std::string, std::string> envs;
    check_compact_disabled(envs, false);

    envs[MANUAL_COMPACT_DISABLED_KEY] = "";
    check_compact_disabled(envs, false);

    envs[MANUAL_COMPACT_DISABLED_KEY] = "true";
    check_compact_disabled(envs, true);

    envs[MANUAL_COMPACT_DISABLED_KEY] = "false";
    check_compact_disabled(envs, false);

    envs[MANUAL_COMPACT_DISABLED_KEY] = "1";
    check_compact_disabled(envs, false);

    envs[MANUAL_COMPACT_DISABLED_KEY] = "0";
    check_compact_disabled(envs, false);

    envs[MANUAL_COMPACT_DISABLED_KEY] = "abc";
    check_compact_disabled(envs, false);
}

TEST_F(manual_compact_service_test, check_once_compact)
{
    // suppose compacted at 1500000000
    set_compact_time(compacted_ts);

    // invalid trigger time
    std::map<std::string, std::string> envs;
    check_once_compact(envs, false);

    envs[MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY] = "";
    check_once_compact(envs, false);

    envs[MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY] = "abc";
    check_once_compact(envs, false);

    envs[MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY] = "-1";
    check_once_compact(envs, false);

    // has been compacted
    envs[MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY] = std::to_string(compacted_ts - 1);
    check_once_compact(envs, false);

    envs[MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY] = std::to_string(compacted_ts);
    check_once_compact(envs, false);

    // has not been compacted
    envs[MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY] = std::to_string(compacted_ts + 1);
    check_once_compact(envs, true);

    envs[MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY] = std::to_string(dsn_now_ms() / 1000);
    check_once_compact(envs, true);
}

TEST_F(manual_compact_service_test, check_periodic_compact)
{
    std::map<std::string, std::string> envs;

    // invalid trigger time format
    check_periodic_compact(envs, false);

    envs[MANUAL_COMPACT_PERIODIC_TRIGGER_TIME_KEY] = "";
    check_periodic_compact(envs, false);

    envs[MANUAL_COMPACT_PERIODIC_TRIGGER_TIME_KEY] = ",";
    check_periodic_compact(envs, false);

    envs[MANUAL_COMPACT_PERIODIC_TRIGGER_TIME_KEY] = "12:oo";
    check_periodic_compact(envs, false);

    envs[MANUAL_COMPACT_PERIODIC_TRIGGER_TIME_KEY] = std::to_string(compacted_ts);
    check_periodic_compact(envs, false);

    // suppose compacted at 10:00
    set_compact_time(dsn::utils::hh_mm_today_to_unix_sec("10:00"));

    // has been compacted
    envs[MANUAL_COMPACT_PERIODIC_TRIGGER_TIME_KEY] = "9:00";
    check_periodic_compact(envs, false);

    envs[MANUAL_COMPACT_PERIODIC_TRIGGER_TIME_KEY] = "3:00,9:00";
    check_periodic_compact(envs, false);

    envs[MANUAL_COMPACT_PERIODIC_TRIGGER_TIME_KEY] = "10:00";
    check_periodic_compact(envs, false);

    // suppose compacted at 09:00
    set_compact_time(dsn::utils::hh_mm_today_to_unix_sec("09:00"));

    // single compact time
    envs[MANUAL_COMPACT_PERIODIC_TRIGGER_TIME_KEY] = "10:00";

    set_mock_now((uint64_t)dsn::utils::hh_mm_today_to_unix_sec("08:00"));
    check_periodic_compact(envs, false);

    set_mock_now((uint64_t)dsn::utils::hh_mm_today_to_unix_sec("09:30"));
    check_periodic_compact(envs, false);

    set_mock_now((uint64_t)dsn::utils::hh_mm_today_to_unix_sec("10:30"));
    check_periodic_compact(envs, true);

    // multiple compact time
    envs[MANUAL_COMPACT_PERIODIC_TRIGGER_TIME_KEY] = "10:00,21:00";

    set_mock_now((uint64_t)dsn::utils::hh_mm_today_to_unix_sec("08:00"));
    check_periodic_compact(envs, false);

    set_mock_now((uint64_t)dsn::utils::hh_mm_today_to_unix_sec("09:30"));
    check_periodic_compact(envs, false);

    set_mock_now((uint64_t)dsn::utils::hh_mm_today_to_unix_sec("10:30"));
    check_periodic_compact(envs, true);

    // suppose compacted at 11:00
    set_compact_time(dsn::utils::hh_mm_today_to_unix_sec("11:00"));

    set_mock_now((uint64_t)dsn::utils::hh_mm_today_to_unix_sec("11:01"));
    check_periodic_compact(envs, false);

    set_mock_now((uint64_t)dsn::utils::hh_mm_today_to_unix_sec("20:30"));
    check_periodic_compact(envs, false);

    set_mock_now((uint64_t)dsn::utils::hh_mm_today_to_unix_sec("21:01"));
    check_periodic_compact(envs, true);

    // suppose compacted at 21:50
    set_compact_time(dsn::utils::hh_mm_today_to_unix_sec("21:50"));

    set_mock_now((uint64_t)dsn::utils::hh_mm_today_to_unix_sec("22:00"));
    check_periodic_compact(envs, false);
}

TEST_F(manual_compact_service_test, extract_manual_compact_opts)
{
    // init _db max level
    set_num_level(7);

    std::map<std::string, std::string> envs;
    rocksdb::CompactRangeOptions out;

    extract_manual_compact_opts(envs, MANUAL_COMPACT_ONCE_KEY_PREFIX, out);
    ASSERT_EQ(out.target_level, -1);
    ASSERT_EQ(out.bottommost_level_compaction, rocksdb::BottommostLevelCompaction::kSkip);

    envs[MANUAL_COMPACT_ONCE_KEY_PREFIX + MANUAL_COMPACT_TARGET_LEVEL_KEY] = "2";
    envs[MANUAL_COMPACT_ONCE_KEY_PREFIX + MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_KEY] =
        MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE;
    extract_manual_compact_opts(envs, MANUAL_COMPACT_ONCE_KEY_PREFIX, out);
    ASSERT_EQ(out.target_level, 2);
    ASSERT_EQ(out.bottommost_level_compaction, rocksdb::BottommostLevelCompaction::kForce);

    envs[MANUAL_COMPACT_ONCE_KEY_PREFIX + MANUAL_COMPACT_TARGET_LEVEL_KEY] = "-1";
    envs[MANUAL_COMPACT_ONCE_KEY_PREFIX + MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_KEY] =
        MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP;
    extract_manual_compact_opts(envs, MANUAL_COMPACT_ONCE_KEY_PREFIX, out);
    ASSERT_EQ(out.target_level, -1);
    ASSERT_EQ(out.bottommost_level_compaction, rocksdb::BottommostLevelCompaction::kSkip);

    envs[MANUAL_COMPACT_ONCE_KEY_PREFIX + MANUAL_COMPACT_TARGET_LEVEL_KEY] = "-2";
    envs[MANUAL_COMPACT_ONCE_KEY_PREFIX + MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_KEY] =
        "nonono";
    extract_manual_compact_opts(envs, MANUAL_COMPACT_ONCE_KEY_PREFIX, out);
    ASSERT_EQ(out.target_level, -1);
    ASSERT_EQ(out.bottommost_level_compaction, rocksdb::BottommostLevelCompaction::kSkip);

    envs[MANUAL_COMPACT_ONCE_KEY_PREFIX + MANUAL_COMPACT_TARGET_LEVEL_KEY] = "8";
    extract_manual_compact_opts(envs, MANUAL_COMPACT_ONCE_KEY_PREFIX, out);
    ASSERT_EQ(out.target_level, -1);
}

TEST_F(manual_compact_service_test, check_manual_compact_state_0_interval)
{
    set_manual_compact_interval(0);

    uint64_t first_time = 1500000000;
    set_mock_now(first_time);

    check_manual_compact_state(true, "1st start ok");
    check_manual_compact_state(false, "1st start not ok");

    manual_compact(first_time, 1);

    check_manual_compact_state(true, "2nd start ok");
    check_manual_compact_state(false, "2nd start not ok");
}

TEST_F(manual_compact_service_test, check_manual_compact_state_1h_interval)
{
    set_manual_compact_interval(3600);

    uint64_t first_time = 1500000000;
    set_mock_now(first_time);
    check_manual_compact_state(true, "1st start ok");
    check_manual_compact_state(false, "1st start not ok");

    manual_compact(first_time, 10); // cost 10 seconds

    set_mock_now(first_time + 1800);
    check_manual_compact_state(false, "1800s past");

    set_mock_now(first_time + 3609);
    check_manual_compact_state(false, "3609s past");

    set_mock_now(first_time + 3610);
    check_manual_compact_state(false, "3610s past");

    set_mock_now(first_time + 3611);
    check_manual_compact_state(true, "3611s past, start ok");
    check_manual_compact_state(false, "3611s past, start not ok");
}

} // namespace server
} // namespace pegasus
