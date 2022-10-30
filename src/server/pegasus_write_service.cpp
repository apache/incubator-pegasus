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

#include "base/pegasus_rpc_types.h"
#include "pegasus_write_service.h"
#include "pegasus_write_service_impl.h"
#include "capacity_unit_calculator.h"

#include "runtime/message_utils.h"
#include "common/replication.codes.h"
#include "utils/defer.h"

namespace pegasus {
namespace server {

DEFINE_TASK_CODE(LPC_INGESTION, TASK_PRIORITY_COMMON, THREAD_POOL_INGESTION)

pegasus_write_service::pegasus_write_service(pegasus_server_impl *server)
    : replica_base(server),
      _server(server),
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

    _pfc_duplicate_qps.init_app_counter("app.pegasus",
                                        fmt::format("duplicate_qps@{}", str_gpid).c_str(),
                                        COUNTER_TYPE_RATE,
                                        "statistic the qps of DUPLICATE requests");

    _pfc_dup_time_lag.init_app_counter(
        "app.pegasus",
        fmt::format("dup.time_lag_ms@{}", app_name()).c_str(),
        COUNTER_TYPE_NUMBER_PERCENTILES,
        "the time (in ms) lag between master and slave in the duplication");

    _dup_lagging_write_threshold_ms = dsn_config_get_value_int64(
        "pegasus.server",
        "dup_lagging_write_threshold_ms",
        10 * 1000,
        "If the duration that a write flows from master to slave is larger than this threshold, "
        "the write is defined a lagging write.");
    _pfc_dup_lagging_writes.init_app_counter(
        "app.pegasus",
        fmt::format("dup.lagging_writes@{}", app_name()).c_str(),
        COUNTER_TYPE_VOLATILE_NUMBER,
        "the number of lagging writes (time lag larger than `dup_lagging_write_threshold_ms`)");
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
        _cu_calculator->add_multi_put_cu(resp.error, update.hash_key, update.kvs);
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
        _cu_calculator->add_multi_remove_cu(resp.error, update.hash_key, update.sort_keys);
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
        _cu_calculator->add_incr_cu(resp.error, update.key);
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
        _cu_calculator->add_check_and_set_cu(resp.error,
                                             update.hash_key,
                                             update.check_sort_key,
                                             update.set_sort_key,
                                             update.set_value);
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
        _cu_calculator->add_check_and_mutate_cu(
            resp.error, update.hash_key, update.check_sort_key, update.mutate_list);
    }

    _pfc_check_and_mutate_latency->set(dsn_now_ns() - start_time);
    return err;
}

void pegasus_write_service::batch_prepare(int64_t decree)
{
    CHECK_EQ_MSG(
        _batch_start_time, 0, "batch_prepare and batch_commit/batch_abort must be called in pair");

    _batch_start_time = dsn_now_ns();
}

int pegasus_write_service::batch_put(const db_write_context &ctx,
                                     const dsn::apps::update_request &update,
                                     dsn::apps::update_response &resp)
{
    CHECK_GT_MSG(_batch_start_time, 0, "batch_put must be called after batch_prepare");

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
    CHECK_GT_MSG(_batch_start_time, 0, "batch_remove must be called after batch_prepare");

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
    CHECK_GT_MSG(_batch_start_time, 0, "batch_commit must be called after batch_prepare");

    int err = _impl->batch_commit(decree);
    clear_up_batch_states();
    return err;
}

void pegasus_write_service::batch_abort(int64_t decree, int err)
{
    CHECK_GT_MSG(_batch_start_time, 0, "batch_abort must be called after batch_prepare");
    CHECK(err, "must abort on non-zero err");

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

int pegasus_write_service::duplicate(int64_t decree,
                                     const dsn::apps::duplicate_request &requests,
                                     dsn::apps::duplicate_response &resp)
{
    // Verifies the cluster_id.
    for (const auto &request : requests.entries) {
        if (!dsn::replication::is_cluster_id_configured(request.cluster_id)) {
            resp.__set_error(rocksdb::Status::kInvalidArgument);
            resp.__set_error_hint("request cluster id is unconfigured");
            return empty_put(decree);
        }
        if (request.cluster_id == get_current_cluster_id()) {
            resp.__set_error(rocksdb::Status::kInvalidArgument);
            resp.__set_error_hint("self-duplicating");
            return empty_put(decree);
        }

        _pfc_duplicate_qps->increment();
        auto cleanup = dsn::defer([this, &request]() {
            uint64_t latency_ms = (dsn_now_us() - request.timestamp) / 1000;
            if (latency_ms > _dup_lagging_write_threshold_ms) {
                _pfc_dup_lagging_writes->increment();
            }
            _pfc_dup_time_lag->set(latency_ms);
        });
        dsn::message_ex *write =
            dsn::from_blob_to_received_msg(request.task_code, request.raw_message);
        bool is_delete = request.task_code == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE ||
                         request.task_code == dsn::apps::RPC_RRDB_RRDB_REMOVE;
        auto remote_timetag = generate_timetag(request.timestamp, request.cluster_id, is_delete);
        auto ctx =
            db_write_context::create_duplicate(decree, remote_timetag, request.verify_timetag);

        if (request.task_code == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
            multi_put_rpc rpc(write);
            resp.__set_error(_impl->multi_put(ctx, rpc.request(), rpc.response()));
            if (resp.error != rocksdb::Status::kOk) {
                return resp.error;
            }
            continue;
        }
        if (request.task_code == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
            multi_remove_rpc rpc(write);
            resp.__set_error(_impl->multi_remove(ctx.decree, rpc.request(), rpc.response()));
            if (resp.error != rocksdb::Status::kOk) {
                return resp.error;
            }
            continue;
        }
        put_rpc put;
        remove_rpc remove;
        if (request.task_code == dsn::apps::RPC_RRDB_RRDB_PUT ||
            request.task_code == dsn::apps::RPC_RRDB_RRDB_REMOVE) {
            int err = 0;
            if (request.task_code == dsn::apps::RPC_RRDB_RRDB_PUT) {
                put = put_rpc(write);
                err = _impl->batch_put(ctx, put.request(), put.response());
            }
            if (request.task_code == dsn::apps::RPC_RRDB_RRDB_REMOVE) {
                remove = remove_rpc(write);
                err = _impl->batch_remove(ctx.decree, remove.request(), remove.response());
            }
            if (!err) {
                err = _impl->batch_commit(ctx.decree);
            } else {
                _impl->batch_abort(ctx.decree, err);
            }
            resp.__set_error(err);
            if (resp.error != rocksdb::Status::kOk) {
                return resp.error;
            }
            continue;
        }
        resp.__set_error(rocksdb::Status::kInvalidArgument);
        resp.__set_error_hint(fmt::format("unrecognized task code {}", request.task_code));
        return empty_put(ctx.decree);
    }
    return resp.error;
}

int pegasus_write_service::ingest_files(int64_t decree,
                                        const dsn::replication::ingestion_request &req,
                                        dsn::replication::ingestion_response &resp)
{
    // TODO(heyuchen): consider cu

    resp.err = dsn::ERR_OK;
    // write empty put to flush decree
    resp.rocksdb_error = empty_put(decree);
    if (resp.rocksdb_error != 0) {
        resp.err = dsn::ERR_TRY_AGAIN;
        return resp.rocksdb_error;
    }

    // ingest files asynchronously
    _server->set_ingestion_status(dsn::replication::ingestion_status::IS_RUNNING);
    dsn::tasking::enqueue(LPC_INGESTION, &_server->_tracker, [this, decree, req]() {
        const auto &err =
            _impl->ingest_files(decree, _server->bulk_load_dir(), req, _server->get_ballot());
        auto status = dsn::replication::ingestion_status::IS_SUCCEED;
        if (err == dsn::ERR_INVALID_VERSION) {
            status = dsn::replication::ingestion_status::IS_INVALID;
        } else if (err != dsn::ERR_OK) {
            status = dsn::replication::ingestion_status::IS_FAILED;
        }
        _server->set_ingestion_status(status);
    });
    return rocksdb::Status::kOk;
}

} // namespace server
} // namespace pegasus
