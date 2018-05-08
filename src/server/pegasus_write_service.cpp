// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_write_service.h"
#include "pegasus_write_service_impl.h"

namespace pegasus {
namespace server {

pegasus_write_service::pegasus_write_service(pegasus_server_impl *server)
    : _impl(new impl(server)), _batch_start_time(0)
{
    std::string str_gpid = fmt::format("{}", server->get_gpid());

    std::string name;

    name = fmt::format("put_qps@{}", str_gpid);
    _pfc_put_qps.init_app_counter(
        "app.pegasus", name.c_str(), COUNTER_TYPE_RATE, "statistic the qps of PUT request");

    name = fmt::format("multi_put_qps@{}", str_gpid);
    _pfc_multi_put_qps.init_app_counter(
        "app.pegasus", name.c_str(), COUNTER_TYPE_RATE, "statistic the qps of MULTI_PUT request");

    name = fmt::format("remove_qps@{}", str_gpid);
    _pfc_remove_qps.init_app_counter(
        "app.pegasus", name.c_str(), COUNTER_TYPE_RATE, "statistic the qps of REMOVE request");

    name = fmt::format("multi_remove_qps@{}", str_gpid);
    _pfc_multi_remove_qps.init_app_counter("app.pegasus",
                                           name.c_str(),
                                           COUNTER_TYPE_RATE,
                                           "statistic the qps of MULTI_REMOVE request");

    name = fmt::format("put_latency@{}", str_gpid);
    _pfc_put_latency.init_app_counter("app.pegasus",
                                      name.c_str(),
                                      COUNTER_TYPE_NUMBER_PERCENTILES,
                                      "statistic the latency of PUT request");

    name = fmt::format("multi_put_latency@{}", str_gpid);
    _pfc_multi_put_latency.init_app_counter("app.pegasus",
                                            name.c_str(),
                                            COUNTER_TYPE_NUMBER_PERCENTILES,
                                            "statistic the latency of MULTI_PUT request");

    name = fmt::format("remove_latency@{}", str_gpid);
    _pfc_remove_latency.init_app_counter("app.pegasus",
                                         name.c_str(),
                                         COUNTER_TYPE_NUMBER_PERCENTILES,
                                         "statistic the latency of REMOVE request");

    name = fmt::format("multi_remove_latency@{}", str_gpid);
    _pfc_multi_remove_latency.init_app_counter("app.pegasus",
                                               name.c_str(),
                                               COUNTER_TYPE_NUMBER_PERCENTILES,
                                               "statistic the latency of MULTI_REMOVE request");
}

pegasus_write_service::~pegasus_write_service() {}

void pegasus_write_service::multi_put(const db_write_context &ctx,
                                      const dsn::apps::multi_put_request &update,
                                      dsn::apps::update_response &resp)
{
    uint64_t start_time = dsn_now_ns();
    _pfc_multi_put_qps->increment();
    _impl->multi_put(ctx, update, resp);
    _pfc_multi_put_latency->set(dsn_now_ns() - start_time);
}

void pegasus_write_service::multi_remove(const db_write_context &ctx,
                                         const dsn::apps::multi_remove_request &update,
                                         dsn::apps::multi_remove_response &resp)
{
    uint64_t start_time = dsn_now_ns();
    _pfc_multi_remove_qps->increment();
    _impl->multi_remove(ctx, update, resp);
    _pfc_multi_remove_latency->set(dsn_now_ns() - start_time);
}

void pegasus_write_service::batch_put(const db_write_context &ctx,
                                      const dsn::apps::update_request &update,
                                      dsn::apps::update_response &resp)
{
    _pfc_put_qps->increment();
    _batch_perfcounters.push_back(_pfc_put_latency.get());

    _impl->batch_put(ctx, update, resp);
}

void pegasus_write_service::batch_remove(const db_write_context &ctx,
                                         const dsn::blob &key,
                                         dsn::apps::update_response &resp)
{
    _pfc_remove_qps->increment();
    _batch_perfcounters.push_back(_pfc_remove_latency.get());

    _impl->batch_remove(ctx, key, resp);
}

int pegasus_write_service::batch_commit(int64_t decree)
{
    dassert(_batch_start_time != 0, "batch_commit and batch_prepare must be called in pair");

    int ret = _impl->batch_commit(decree);

    uint64_t latency = dsn_now_ns() - _batch_start_time;
    for (dsn::perf_counter *pfc : _batch_perfcounters) {
        pfc->set(latency);
    }

    _batch_perfcounters.clear();
    _batch_start_time = 0;

    return ret;
}

void pegasus_write_service::batch_prepare()
{
    dassert(_batch_start_time == 0, "batch_commit and batch_prepare must be called in pair");

    _batch_start_time = dsn_now_ns();
}

int pegasus_write_service::empty_put(const db_write_context &ctx)
{
    std::string empty_key, empty_value;
    _impl->db_write_batch_put(ctx, empty_key, empty_value, 0);
    return _impl->db_write(ctx.decree);
}

} // namespace server
} // namespace pegasus
