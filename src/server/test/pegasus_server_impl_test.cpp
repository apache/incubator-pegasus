// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_test_base.h"
#include "message_utils.h"

#include "server/pegasus_server_write.h"
#include "base/pegasus_key_schema.h"

namespace pegasus {
namespace server {

class pegasus_server_write_test : public pegasus_server_test_base
{
    std::unique_ptr<pegasus_server_write> _server_write;

public:
    pegasus_server_write_test() : pegasus_server_test_base()
    {
        _server_write = dsn::make_unique<pegasus_server_write>(_server.get());
    }

    void test_duplicate_not_batched()
    {
        std::string hash_key = "hash_key";
        constexpr int kv_num = 100;
        std::string sort_key[kv_num];
        std::string value[kv_num];

        for (int i = 0; i < 100; i++) {
            sort_key[i] = "sort_key_" + std::to_string(i);
            value[i] = "value_" + std::to_string(i);
        }

        dsn::apps::duplicate_request duplicate;
        duplicate.timetag = 1000;
        dsn::apps::duplicate_response resp;

        {
            dsn::apps::multi_put_request mput;
            for (int i = 0; i < 100; i++) {
                mput.kvs.emplace_back();
                mput.kvs.back().key.assign(sort_key[i].data(), 0, sort_key[i].size());
                mput.kvs.back().value.assign(value[i].data(), 0, value[i].size());
            }
            dsn_message_t mput_msg = pegasus::create_multi_put_request(mput);

            duplicate.task_code = dsn::apps::RPC_RRDB_RRDB_MULTI_PUT;
            duplicate.raw_message = dsn::move_message_to_blob(mput_msg);

            _server_write->on_duplicate_impl(false, duplicate, resp);
            ASSERT_EQ(resp.error, 0);
        }

        {
            dsn::apps::multi_remove_request mremove;
            for (int i = 0; i < 100; i++) {
                mremove.sort_keys.emplace_back();
                mremove.sort_keys.back().assign(sort_key[i].data(), 0, sort_key[i].size());
            }
            dsn_message_t mremove_msg = pegasus::create_multi_remove_request(mremove);

            duplicate.task_code = dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE;
            duplicate.raw_message = dsn::move_message_to_blob(mremove_msg);

            _server_write->on_duplicate_impl(false, duplicate, resp);
            ASSERT_EQ(resp.error, 0);
        }
    }

    void test_duplicate_batched()
    {
        std::string hash_key = "hash_key";
        constexpr int kv_num = 100;
        std::string sort_key[kv_num];
        std::string value[kv_num];

        for (int i = 0; i < 100; i++) {
            sort_key[i] = "sort_key_" + std::to_string(i);
            value[i] = "value_" + std::to_string(i);
        }

        {
            dsn::apps::duplicate_request duplicate;
            duplicate.timetag = 1000;
            dsn::apps::duplicate_response resp;

            for (int i = 0; i < kv_num; i++) {
                dsn::apps::update_request request;
                pegasus::pegasus_generate_key(request.key, hash_key, sort_key[i]);
                request.value.assign(value[i].data(), 0, value[i].size());

                duplicate.raw_message =
                    dsn::move_message_to_blob(pegasus::create_put_request(request));
                duplicate.task_code = dsn::apps::RPC_RRDB_RRDB_PUT;
                _server_write->on_duplicate_impl(true, duplicate, resp);
                ASSERT_EQ(resp.error, 0);
            }
        }
    }
};

TEST_F(pegasus_server_write_test, duplicate_not_batched) { test_duplicate_not_batched(); }

TEST_F(pegasus_server_write_test, duplicate_batched) { test_duplicate_batched(); }

} // namespace server
} // namespace pegasus
