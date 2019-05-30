// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_write_service.h"
#include "pegasus_write_service_impl.h"
#include "capacity_unit_calculator.h"

namespace pegasus {
namespace server {

pegasus_write_service::pegasus_write_service(pegasus_server_impl *server)
    : _server(server),
      _impl(new impl(server)),
      _batch_start_time(0),
      _cu_calculator(server->_cu_calculator.get())
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

    name = fmt::format("incr_qps@{}", str_gpid);
    _pfc_incr_qps.init_app_counter(
        "app.pegasus", name.c_str(), COUNTER_TYPE_RATE, "statistic the qps of INCR request");

    name = fmt::format("check_and_set_qps@{}", str_gpid);
    _pfc_check_and_set_qps.init_app_counter("app.pegasus",
                                            name.c_str(),
                                            COUNTER_TYPE_RATE,
                                            "statistic the qps of CHECK_AND_SET request");

    name = fmt::format("check_and_mutate_qps@{}", str_gpid);
    _pfc_check_and_mutate_qps.init_app_counter("app.pegasus",
                                               name.c_str(),
                                               COUNTER_TYPE_RATE,
                                               "statistic the qps of CHECK_AND_MUTATE request");

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

    name = fmt::format("incr_latency@{}", str_gpid);
    _pfc_incr_latency.init_app_counter("app.pegasus",
                                       name.c_str(),
                                       COUNTER_TYPE_NUMBER_PERCENTILES,
                                       "statistic the latency of INCR request");

    name = fmt::format("check_and_set_latency@{}", str_gpid);
    _pfc_check_and_set_latency.init_app_counter("app.pegasus",
                                                name.c_str(),
                                                COUNTER_TYPE_NUMBER_PERCENTILES,
                                                "statistic the latency of CHECK_AND_SET request");

    name = fmt::format("check_and_mutate_latency@{}", str_gpid);
    _pfc_check_and_mutate_latency.init_app_counter(
        "app.pegasus",
        name.c_str(),
        COUNTER_TYPE_NUMBER_PERCENTILES,
        "statistic the latency of CHECK_AND_MUTATE request");

    _pfc_duplicated_put_qps.init_app_counter("app.pegasus",
                                             fmt::format("duplicated_put_qps@{}", str_gpid).c_str(),
                                             COUNTER_TYPE_RATE,
                                             "statistic the qps of duplicated PUT request");

    _pfc_duplicated_multi_put_qps.init_app_counter(
        "app.pegasus",
        fmt::format("duplicated_multi_put_qps@{}", str_gpid).c_str(),
        COUNTER_TYPE_RATE,
        "statistic the qps of duplicated MULTI_PUT request");

    _pfc_duplicated_remove_qps.init_app_counter(
        "app.pegasus",
        fmt::format("duplicated_remove_qps@{}", str_gpid).c_str(),
        COUNTER_TYPE_RATE,
        "statistic the qps of duplicated REMOVE request");

    _pfc_duplicated_multi_remove_qps.init_app_counter(
        "app.pegasus",
        fmt::format("duplicated_multi_remove_qps@{}", str_gpid).c_str(),
        COUNTER_TYPE_RATE,
        "statistic the qps of duplicated MULTI_REMOVE request");
}

pegasus_write_service::~pegasus_write_service() {}

int pegasus_write_service::empty_put(int64_t decree) { return _impl->empty_put(decree); }

int pegasus_write_service::multi_put(const db_write_context &ctx,
                                     const dsn::apps::multi_put_request &update,
                                     dsn::apps::update_response &resp)
{
    uint64_t start_time = dsn_now_ns();
    _pfc_multi_put_qps->increment();
    int err = _impl->multi_put(ctx, update, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_multi_put_cu(resp.error, update.kvs);
    }

    _pfc_multi_put_latency->set(dsn_now_ns() - start_time);
    return err;
}

int pegasus_write_service::multi_remove(int64_t decree,
                                        const dsn::apps::multi_remove_request &update,
                                        dsn::apps::multi_remove_response &resp)
{
    uint64_t start_time = dsn_now_ns();
    _pfc_multi_remove_qps->increment();
    int err = _impl->multi_remove(decree, update, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_multi_remove_cu(resp.error, update.sort_keys);
    }

    _pfc_multi_remove_latency->set(dsn_now_ns() - start_time);
    return err;
}

int pegasus_write_service::incr(int64_t decree,
                                const dsn::apps::incr_request &update,
                                dsn::apps::incr_response &resp)
{
    uint64_t start_time = dsn_now_ns();
    _pfc_incr_qps->increment();
    int err = _impl->incr(decree, update, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_incr_cu(resp.error);
    }

    _pfc_incr_latency->set(dsn_now_ns() - start_time);
    return err;
}

int pegasus_write_service::check_and_set(int64_t decree,
                                         const dsn::apps::check_and_set_request &update,
                                         dsn::apps::check_and_set_response &resp)
{
    uint64_t start_time = dsn_now_ns();
    _pfc_check_and_set_qps->increment();
    int err = _impl->check_and_set(decree, update, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_check_and_set_cu(resp.error, update.set_sort_key, update.set_value);
    }

    _pfc_check_and_set_latency->set(dsn_now_ns() - start_time);
    return err;
}

int pegasus_write_service::check_and_mutate(int64_t decree,
                                            const dsn::apps::check_and_mutate_request &update,
                                            dsn::apps::check_and_mutate_response &resp)
{
    uint64_t start_time = dsn_now_ns();
    _pfc_check_and_mutate_qps->increment();
    int err = _impl->check_and_mutate(decree, update, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_check_and_mutate_cu(resp.error, update.mutate_list);
    }

    _pfc_check_and_mutate_latency->set(dsn_now_ns() - start_time);
    return err;
}

void pegasus_write_service::batch_prepare(int64_t decree)
{
    dassert(_batch_start_time == 0,
            "batch_prepare and batch_commit/batch_abort must be called in pair");

    _batch_start_time = dsn_now_ns();
}

int pegasus_write_service::batch_put(const db_write_context &ctx,
                                     const dsn::apps::update_request &update,
                                     dsn::apps::update_response &resp)
{
    dassert(_batch_start_time != 0, "batch_put must be called after batch_prepare");

    _batch_qps_perfcounters.push_back(_pfc_put_qps.get());
    _batch_latency_perfcounters.push_back(_pfc_put_latency.get());
    int err = _impl->batch_put(ctx, update, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_put_cu(resp.error, update.key, update.value);
    }

    return err;
}

int pegasus_write_service::batch_remove(int64_t decree,
                                        const dsn::blob &key,
                                        dsn::apps::update_response &resp)
{
    dassert(_batch_start_time != 0, "batch_remove must be called after batch_prepare");

    _batch_qps_perfcounters.push_back(_pfc_remove_qps.get());
    _batch_latency_perfcounters.push_back(_pfc_remove_latency.get());
    int err = _impl->batch_remove(decree, key, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_remove_cu(resp.error, key);
    }

    return err;
}

int pegasus_write_service::batch_commit(int64_t decree)
{
    dassert(_batch_start_time != 0, "batch_commit must be called after batch_prepare");

    int err = _impl->batch_commit(decree);
    clear_up_batch_states();
    return err;
}

void pegasus_write_service::batch_abort(int64_t decree, int err)
{
    dassert(_batch_start_time != 0, "batch_abort must be called after batch_prepare");
    dassert(err, "must abort on non-zero err");

    _impl->batch_abort(decree, err);
    clear_up_batch_states();
}

void pegasus_write_service::set_default_ttl(uint32_t ttl) { _impl->set_default_ttl(ttl); }

void pegasus_write_service::clear_up_batch_states()
{
    uint64_t latency = dsn_now_ns() - _batch_start_time;
    for (dsn::perf_counter *pfc : _batch_qps_perfcounters)
        pfc->increment();
    for (dsn::perf_counter *pfc : _batch_latency_perfcounters)
        pfc->set(latency);

    _batch_qps_perfcounters.clear();
    _batch_latency_perfcounters.clear();
    _batch_start_time = 0;
}

int pegasus_write_service::duplicated_multi_put(const db_write_context &ctx,
                                                const dsn::apps::multi_put_request &update,
                                                dsn::apps::update_response &resp)
{
    uint64_t start_time = dsn_now_ns();
    _pfc_duplicated_multi_put_qps->increment();
    int err = _impl->multi_put(ctx, update, resp);
    _pfc_multi_put_latency->set(dsn_now_ns() - start_time);
    return err;
}

int pegasus_write_service::duplicated_multi_remove(int64_t decree,
                                                   const dsn::apps::multi_remove_request &update,
                                                   dsn::apps::multi_remove_response &resp)
{
    uint64_t start_time = dsn_now_ns();
    _pfc_duplicated_multi_remove_qps->increment();
    int err = _impl->multi_remove(decree, update, resp);
    _pfc_multi_remove_latency->set(dsn_now_ns() - start_time);
    return err;
}

int pegasus_write_service::duplicated_put(const db_write_context &ctx,
                                          const dsn::apps::update_request &update,
                                          dsn::apps::update_response &resp)
{
    uint64_t start_time = dsn_now_ns();
    _pfc_duplicated_put_qps->increment();
    int err = _impl->batch_put(ctx, update, resp);
    if (!err) {
        err = _impl->batch_commit(ctx.decree);
    } else {
        _impl->batch_abort(ctx.decree, err);
    }
    _pfc_put_latency->set(dsn_now_ns() - start_time);
    return err;
}

int pegasus_write_service::duplicated_remove(int64_t decree,
                                             const dsn::blob &key,
                                             dsn::apps::update_response &resp)
{
    uint64_t start_time = dsn_now_ns();
    _pfc_duplicated_remove_qps->increment();
    int err = _impl->batch_remove(decree, key, resp);
    if (!err) {
        err = _impl->batch_commit(decree);
    } else {
        _impl->batch_abort(decree, err);
    }
    _pfc_remove_latency->set(dsn_now_ns() - start_time);
    return err;
}

} // namespace server
} // namespace pegasus
