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

#include <dsn/dist/fmt_logging.h>

#include "dist/replication/meta_server/meta_split_service.h"
#include "dist/replication/meta_server/meta_state_service_utils.h"

namespace dsn {
namespace replication {

meta_split_service::meta_split_service(meta_service *meta_srv)
{
    _meta_svc = meta_srv;
    _state = meta_srv->get_server_state();
}

void meta_split_service::app_partition_split(app_partition_split_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    {
        zauto_write_lock l(app_lock());

        app = _state->get_app(request.app_name);
        if (!app || app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_APP_NOT_EXIST;
            dwarn_f("client({}) sent split request with invalid app({}), app is not existed or "
                    "unavailable",
                    rpc.remote_address().to_string(),
                    request.app_name);
            return;
        }

        response.app_id = app->app_id;
        response.partition_count = app->partition_count;

        // new_partition_count != old_partition_count*2
        if (request.new_partition_count != app->partition_count * 2) {
            response.err = ERR_INVALID_PARAMETERS;
            dwarn_f("client({}) sent split request with wrong partition count: app({}), partition "
                    "count({}),"
                    "new_partition_count({})",
                    rpc.remote_address().to_string(),
                    request.app_name,
                    app->partition_count,
                    request.new_partition_count);
            return;
        }

        for (const auto &partition_config : app->partitions) {
            // partition already during split
            if (partition_config.ballot < 0) {
                response.err = ERR_BUSY;
                dwarn_f("app is already during partition split, client({}) sent repeated split "
                        "request: app({}), new_partition_count({})",
                        rpc.remote_address().to_string(),
                        request.app_name,
                        request.new_partition_count);
                return;
            }
        }
    }

    ddebug_f("app({}) start to partition split, new_partition_count={}",
             request.app_name,
             request.new_partition_count);

    do_app_partition_split(std::move(app), std::move(rpc));
}

void meta_split_service::do_app_partition_split(std::shared_ptr<app_state> app,
                                                app_partition_split_rpc rpc)
{
    auto on_write_storage_complete = [app, rpc, this]() {
        ddebug_f("app({}) update partition count on remote storage, new partition count is {}",
                 app->app_name.c_str(),
                 app->partition_count * 2);

        zauto_write_lock l(app_lock());
        app->partition_count *= 2;
        app->helpers->contexts.resize(app->partition_count);
        app->partitions.resize(app->partition_count);

        for (int i = 0; i < app->partition_count; ++i) {
            app->helpers->contexts[i].config_owner = &app->partitions[i];
            if (i >= app->partition_count / 2) {
                app->partitions[i].ballot = invalid_ballot;
                app->partitions[i].pid = gpid(app->app_id, i);
            }
        }

        auto &response = rpc.response();
        response.err = ERR_OK;
        response.partition_count = app->partition_count;
    };

    if (app->init_partition_count <= 0) {
        app->init_partition_count = app->partition_count;
    }
    auto copy = *app;
    copy.partition_count *= 2;
    blob value = dsn::json::json_forwarder<app_info>::encode(copy);

    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), on_write_storage_complete);
}
} // namespace replication
} // namespace dsn
