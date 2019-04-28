// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "result_writer.h"

namespace pegasus {
namespace server {

DEFINE_TASK_CODE(LPC_WRITE_RESULT, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

result_writer::result_writer(const std::string &cluster_name, const std::string app_name)
    : _cluster_name(cluster_name), _app_name(app_name), _client(nullptr)
{
    // initialize the _client.
    if (!pegasus_client_factory::initialize(nullptr)) {
        dassert(false, "Initialize the pegasus client failed");
    }
    _client = pegasus_client_factory::get_client(_cluster_name.c_str(), _app_name.c_str());
    dassert(_client != nullptr, "Initialize the _client failed");
}

result_writer::~result_writer()
{
    _tracker.cancel_outstanding_tasks();
    // don't delete _client, just set _client to nullptr.
    _client = nullptr;
}

pegasus_client *result_writer::get_client() { return _client; }

void result_writer::set_result(const std::string &hash_key,
                               const std::string &sort_key,
                               const std::string &value,
                               int try_count)
{
    auto async_set_callback = [=](int err, pegasus_client::internal_info &&info) {
        if (err != PERR_OK) {
            int new_try_count = try_count - 1;
            if (new_try_count > 0) {
                derror("set_result fail, hash_key = %s, sort_key = %s, value = %s, "
                       "error = %s, left_try_count = %d, try again after 1 minute",
                       hash_key.c_str(),
                       sort_key.c_str(),
                       value.c_str(),
                       _client->get_error_string(err),
                       new_try_count);
                ::dsn::tasking::enqueue(
                    LPC_WRITE_RESULT,
                    &_tracker,
                    [=]() { set_result(hash_key, sort_key, value, new_try_count); },
                    0,
                    std::chrono::minutes(1));
            } else {
                derror("set_result fail, hash_key = %s, sort_key = %s, value = %s, "
                       "error = %s, left_try_count = %d, do not try again",
                       hash_key.c_str(),
                       sort_key.c_str(),
                       value.c_str(),
                       _client->get_error_string(err),
                       new_try_count);
            }
        } else {
            dinfo("set_result succeed, hash_key = %s, sort_key = %s, value = %s",
                  hash_key.c_str(),
                  sort_key.c_str(),
                  value.c_str());
        }
    };

    _client->async_set(hash_key, sort_key, value, std::move(async_set_callback));
}
}
} // namespace
