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

#include <fmt/core.h>
#include <rocksdb/status.h>
#include <algorithm>
#include <functional>
#include <set>
#include <string_view>
#include <vector>

#include "base/pegasus_rpc_types.h"
#include "bulk_load_types.h"
#include "capacity_unit_calculator.h"
#include "common/duplication_common.h"
#include "common/replication.codes.h"
#include "duplication_internal_types.h"
#include "pegasus_value_schema.h"
#include "pegasus_write_service.h"
#include "pegasus_write_service_impl.h"
#include "rrdb/rrdb.code.definition.h"
#include "rrdb/rrdb_types.h"
#include "runtime/api_layer1.h"
#include "runtime/message_utils.h"
#include "server/pegasus_server_impl.h"
#include "task/async_calls.h"
#include "task/task_code.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"

METRIC_DEFINE_counter(replica,
                      put_requests,
                      dsn::metric_unit::kRequests,
                      "The number of PUT requests");

METRIC_DEFINE_counter(replica,
                      multi_put_requests,
                      dsn::metric_unit::kRequests,
                      "The number of MULTI_PUT requests");

METRIC_DEFINE_counter(replica,
                      remove_requests,
                      dsn::metric_unit::kRequests,
                      "The number of REMOVE requests");

METRIC_DEFINE_counter(replica,
                      multi_remove_requests,
                      dsn::metric_unit::kRequests,
                      "The number of MULTI_REMOVE requests");

METRIC_DEFINE_counter(replica,
                      incr_requests,
                      dsn::metric_unit::kRequests,
                      "The number of INCR requests");

METRIC_DEFINE_counter(replica,
                      check_and_set_requests,
                      dsn::metric_unit::kRequests,
                      "The number of CHECK_AND_SET requests");

METRIC_DEFINE_counter(replica,
                      check_and_mutate_requests,
                      dsn::metric_unit::kRequests,
                      "The number of CHECK_AND_MUTATE requests");

METRIC_DEFINE_percentile_int64(replica,
                               make_incr_idempotent_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The duration that an incr request is made idempotent, "
                               "including reading the current value from storage engine, "
                               "increasing it by a given amount and translating the incr "
                               "request into the single-put request. Only used for the "
                               "primary replicas");

METRIC_DEFINE_percentile_int64(replica,
                               make_check_and_set_idempotent_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The duration that a check_and_set request is made "
                               "idempotent, including reading the check value from "
                               "storage engine, validating the check conditions and "
                               "translating the check_and_set request into the single-put "
                               "request. Only used for the primary replicas");

METRIC_DEFINE_percentile_int64(replica,
                               make_check_and_mutate_idempotent_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The duration that a check_and_mutate request is made "
                               "idempotent, including reading the check value from "
                               "storage engine, validating the check conditions and "
                               "translating the check_and_mutate request into multiple "
                               "single-put and single-remove requests. Only used for the "
                               "primary replicas");

METRIC_DEFINE_percentile_int64(replica,
                               put_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency of PUT requests");

METRIC_DEFINE_percentile_int64(replica,
                               multi_put_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency of MULTI_PUT requests");

METRIC_DEFINE_percentile_int64(replica,
                               remove_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency of REMOVE requests");

METRIC_DEFINE_percentile_int64(replica,
                               multi_remove_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency of MULTI_REMOVE requests");

METRIC_DEFINE_percentile_int64(replica,
                               incr_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency of INCR requests");

METRIC_DEFINE_percentile_int64(replica,
                               check_and_set_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency of CHECK_AND_SET requests");

METRIC_DEFINE_percentile_int64(replica,
                               check_and_mutate_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency of CHECK_AND_MUTATE requests");

METRIC_DEFINE_counter(replica,
                      dup_requests,
                      dsn::metric_unit::kRequests,
                      "The number of DUPLICATE requests");

METRIC_DEFINE_percentile_int64(replica,
                               dup_time_lag_ms,
                               dsn::metric_unit::kMilliSeconds,
                               "the time lag (in ms) between master and slave in the duplication");

METRIC_DEFINE_counter(
    replica,
    dup_lagging_writes,
    dsn::metric_unit::kRequests,
    "the number of lagging writes (time lag larger than `dup_lagging_write_threshold_ms`)");

DSN_DEFINE_int64(pegasus.server,
                 dup_lagging_write_threshold_ms,
                 10 * 1000,
                 "If the duration that a write flows from master to slave is larger than this "
                 "threshold, the write is defined a lagging write.");

namespace dsn {
class blob;
class message_ex;
} // namespace dsn

namespace pegasus::server {

DEFINE_TASK_CODE(LPC_INGESTION, TASK_PRIORITY_COMMON, THREAD_POOL_INGESTION)

pegasus_write_service::pegasus_write_service(pegasus_server_impl *server)
    : replica_base(server),
      _server(server),
      _impl(new impl(server)),
      _batch_start_time(0),
      _cu_calculator(server->_cu_calculator.get()),
      METRIC_VAR_INIT_replica(put_requests),
      METRIC_VAR_INIT_replica(multi_put_requests),
      METRIC_VAR_INIT_replica(remove_requests),
      METRIC_VAR_INIT_replica(multi_remove_requests),
      METRIC_VAR_INIT_replica(incr_requests),
      METRIC_VAR_INIT_replica(check_and_set_requests),
      METRIC_VAR_INIT_replica(check_and_mutate_requests),
      METRIC_VAR_INIT_replica(make_incr_idempotent_latency_ns),
      METRIC_VAR_INIT_replica(make_check_and_set_idempotent_latency_ns),
      METRIC_VAR_INIT_replica(make_check_and_mutate_idempotent_latency_ns),
      METRIC_VAR_INIT_replica(put_latency_ns),
      METRIC_VAR_INIT_replica(multi_put_latency_ns),
      METRIC_VAR_INIT_replica(remove_latency_ns),
      METRIC_VAR_INIT_replica(multi_remove_latency_ns),
      METRIC_VAR_INIT_replica(incr_latency_ns),
      METRIC_VAR_INIT_replica(check_and_set_latency_ns),
      METRIC_VAR_INIT_replica(check_and_mutate_latency_ns),
      METRIC_VAR_INIT_replica(dup_requests),
      METRIC_VAR_INIT_replica(dup_time_lag_ms),
      METRIC_VAR_INIT_replica(dup_lagging_writes)
{
}

pegasus_write_service::~pegasus_write_service() {}

int pegasus_write_service::empty_put(int64_t decree) { return _impl->empty_put(decree); }

int pegasus_write_service::multi_put(const db_write_context &ctx,
                                     const dsn::apps::multi_put_request &update,
                                     dsn::apps::update_response &resp)
{
    METRIC_VAR_AUTO_LATENCY(multi_put_latency_ns);
    METRIC_VAR_INCREMENT(multi_put_requests);

    int err = _impl->multi_put(ctx, update, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_multi_put_cu(resp.error, update.hash_key, update.kvs);
    }

    return err;
}

int pegasus_write_service::multi_remove(int64_t decree,
                                        const dsn::apps::multi_remove_request &update,
                                        dsn::apps::multi_remove_response &resp)
{
    METRIC_VAR_AUTO_LATENCY(multi_remove_latency_ns);
    METRIC_VAR_INCREMENT(multi_remove_requests);

    int err = _impl->multi_remove(decree, update, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_multi_remove_cu(resp.error, update.hash_key, update.sort_keys);
    }

    return err;
}

int pegasus_write_service::make_idempotent(const dsn::apps::incr_request &req,
                                           dsn::apps::incr_response &err_resp,
                                           std::vector<dsn::apps::update_request> &updates)
{
    METRIC_VAR_AUTO_LATENCY(make_incr_idempotent_latency_ns);

    return _impl->make_idempotent(req, err_resp, updates);
}

int pegasus_write_service::put(const db_write_context &ctx,
                               const std::vector<dsn::apps::update_request> &updates,
                               const dsn::apps::incr_request &req,
                               dsn::apps::incr_response &resp)
{
    METRIC_VAR_AUTO_LATENCY(incr_latency_ns);
    METRIC_VAR_INCREMENT(incr_requests);

    const int err = _impl->put(ctx, updates, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_incr_cu(resp.error, req.key);
    }

    return err;
}

int pegasus_write_service::incr(int64_t decree,
                                const dsn::apps::incr_request &update,
                                dsn::apps::incr_response &resp)
{
    METRIC_VAR_AUTO_LATENCY(incr_latency_ns);
    METRIC_VAR_INCREMENT(incr_requests);

    int err = _impl->incr(decree, update, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_incr_cu(resp.error, update.key);
    }

    return err;
}

int pegasus_write_service::make_idempotent(const dsn::apps::check_and_set_request &req,
                                           dsn::apps::check_and_set_response &err_resp,
                                           std::vector<dsn::apps::update_request> &updates)
{
    METRIC_VAR_AUTO_LATENCY(make_check_and_set_idempotent_latency_ns);

    return _impl->make_idempotent(req, err_resp, updates);
}

int pegasus_write_service::put(const db_write_context &ctx,
                               const std::vector<dsn::apps::update_request> &updates,
                               const dsn::apps::check_and_set_request &req,
                               dsn::apps::check_and_set_response &resp)
{
    METRIC_VAR_AUTO_LATENCY(check_and_set_latency_ns);
    METRIC_VAR_INCREMENT(check_and_set_requests);

    const int err = _impl->put(ctx, updates, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_check_and_set_cu(
            resp.error, req.hash_key, req.check_sort_key, req.set_sort_key, req.set_value);
    }

    return err;
}

int pegasus_write_service::check_and_set(int64_t decree,
                                         const dsn::apps::check_and_set_request &update,
                                         dsn::apps::check_and_set_response &resp)
{
    METRIC_VAR_AUTO_LATENCY(check_and_set_latency_ns);
    METRIC_VAR_INCREMENT(check_and_set_requests);

    int err = _impl->check_and_set(decree, update, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_check_and_set_cu(resp.error,
                                             update.hash_key,
                                             update.check_sort_key,
                                             update.set_sort_key,
                                             update.set_value);
    }

    return err;
}

int pegasus_write_service::make_idempotent(const dsn::apps::check_and_mutate_request &req,
                                           dsn::apps::check_and_mutate_response &err_resp,
                                           std::vector<dsn::apps::update_request> &updates)
{
    METRIC_VAR_AUTO_LATENCY(make_check_and_mutate_idempotent_latency_ns);

    return _impl->make_idempotent(req, err_resp, updates);
}

int pegasus_write_service::put(const db_write_context &ctx,
                               const std::vector<dsn::apps::update_request> &updates,
                               const dsn::apps::check_and_mutate_request &req,
                               dsn::apps::check_and_mutate_response &resp)
{
    METRIC_VAR_AUTO_LATENCY(check_and_mutate_latency_ns);
    METRIC_VAR_INCREMENT(check_and_mutate_requests);

    const int err = _impl->put(ctx, updates, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_check_and_mutate_cu(
            resp.error, req.hash_key, req.check_sort_key, req.mutate_list);
    }

    return err;
}

int pegasus_write_service::check_and_mutate(int64_t decree,
                                            const dsn::apps::check_and_mutate_request &update,
                                            dsn::apps::check_and_mutate_response &resp)
{
    METRIC_VAR_AUTO_LATENCY(check_and_mutate_latency_ns);
    METRIC_VAR_INCREMENT(check_and_mutate_requests);

    int err = _impl->check_and_mutate(decree, update, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_check_and_mutate_cu(
            resp.error, update.hash_key, update.check_sort_key, update.mutate_list);
    }

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
    // `kBatchWriteTypeMap` is used to map dsn::apps::update_type field in the single put
    // request to batch_write_type, to measure the size of requests in batch for each kind
    // of write into `_batch_sizes`.
    static constexpr std::array kBatchWriteTypeMap = {
        batch_write_type::put,
        batch_write_type::incr,
        batch_write_type::check_and_set,
        batch_write_type::check_and_mutate,
        batch_write_type::check_and_mutate,
    };
    static_assert(dsn::apps::update_type::UT_CHECK_AND_MUTATE_REMOVE + 1 ==
                      kBatchWriteTypeMap.size(),
                  "kBatchWriteTypeMap does not match dsn::apps::update_type");

    CHECK_GT_MSG(_batch_start_time, 0, "batch_put must be called after batch_prepare");

    if (!update.__isset.type) {
        // This is a general single-put request.
        ++put_batch_size();
    } else {
        // There are only two possible situations for batch_put() where this put request
        // originates from an atomic write request:
        // - now we are replaying plog into RocksDB at startup of this replica.
        // - now we are in a secondary replica: just received a prepare request and appended
        // it to plog, now we are applying it into RocksDB.
        //
        // Though this is a put request, we choose to udapte the metrics of its original
        // request (i.e. the atomic write).
        if (dsn_likely(update.type < kBatchWriteTypeMap.size())) {
            ++batch_size(kBatchWriteTypeMap.at(update.type));
        }
    }

    const int err = _impl->batch_put(ctx, update, resp);

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

    ++remove_batch_size();

    const int err = _impl->batch_remove(decree, key, resp);

    if (_server->is_primary()) {
        _cu_calculator->add_remove_cu(resp.error, key);
    }

    return err;
}

int pegasus_write_service::batch_commit(int64_t decree)
{
    CHECK_GT_MSG(_batch_start_time, 0, "batch_commit must be called after batch_prepare");

    const int err = _impl->batch_commit(decree);
    batch_finish();
    return err;
}

void pegasus_write_service::batch_abort(int64_t decree, int err)
{
    CHECK_GT_MSG(_batch_start_time, 0, "batch_abort must be called after batch_prepare");
    CHECK(err, "must abort on non-zero err");

    _impl->batch_abort(decree, err);
    batch_finish();
}

void pegasus_write_service::set_default_ttl(uint32_t ttl) { _impl->set_default_ttl(ttl); }

void pegasus_write_service::batch_finish()
{
#define UPDATE_BATCH_METRICS(op, nrequests)                                                        \
    do {                                                                                           \
        const auto __nrequests = (nrequests);                                                      \
        METRIC_VAR_INCREMENT_BY(op##_requests, static_cast<int64_t>(__nrequests));                 \
        METRIC_VAR_SET(op##_latency_ns, static_cast<size_t>(__nrequests), latency_ns);             \
    } while (0)

#define UPDATE_BATCH_METRICS_FOR_SINGLE_WRITE(op)                                                  \
    UPDATE_BATCH_METRICS(op, batch_size(batch_write_type::op))

    const auto latency_ns = static_cast<int64_t>(dsn_now_ns() - _batch_start_time);

    // Take the latency of executing the entire batch as the latency for processing each
    // request within it, since the latency of each request could not be known.
    UPDATE_BATCH_METRICS_FOR_SINGLE_WRITE(put);
    UPDATE_BATCH_METRICS_FOR_SINGLE_WRITE(remove);

    // These idempotent updates are translated from the atomic write requests. See comments
    // in batch_put() for the two possible situations where we are now.
    //
    // A batch only contains one update translated from an incr or check_and_set request.
    // However, a batch may contain multiple updates translated from a check_and_mutate.
    // Therefore, we need to measure the number of check_and_mutate requests:
    // - 1 if the number of idempotent updates is at least 1;
    // - 0 otherwise.
    UPDATE_BATCH_METRICS_FOR_SINGLE_WRITE(incr);
    UPDATE_BATCH_METRICS_FOR_SINGLE_WRITE(check_and_set);
    UPDATE_BATCH_METRICS(check_and_mutate,
                         std::min(1U, batch_size(batch_write_type::check_and_mutate)));

    // Reset _batch_sizes for next calculation.
    _batch_sizes.fill(0);

    _batch_start_time = 0;

#undef UPDATE_BATCH_METRICS_FOR_SINGLE_WRITE
#undef UPDATE_BATCH_METRICS
}

int pegasus_write_service::duplicate(int64_t decree,
                                     const dsn::apps::duplicate_request &update,
                                     dsn::apps::duplicate_response &resp)
{
    // Verifies the cluster_id.
    for (const auto &request : update.entries) {
        if (!dsn::replication::is_dup_cluster_id_configured(request.cluster_id)) {
            resp.__set_error(rocksdb::Status::kInvalidArgument);
            resp.__set_error_hint("request cluster id is unconfigured");
            return empty_put(decree);
        }
        if (request.cluster_id == dsn::replication::get_current_dup_cluster_id()) {
            resp.__set_error(rocksdb::Status::kInvalidArgument);
            resp.__set_error_hint("self-duplicating");
            return empty_put(decree);
        }

        METRIC_VAR_INCREMENT(dup_requests);
        METRIC_VAR_AUTO_LATENCY(
            dup_time_lag_ms, request.timestamp * 1000, [this](uint64_t latency) {
                if (latency > FLAGS_dup_lagging_write_threshold_ms) {
                    METRIC_VAR_INCREMENT(dup_lagging_writes);
                }
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
            int err = rocksdb::Status::kOk;
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
    if (resp.rocksdb_error != rocksdb::Status::kOk) {
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

} // namespace pegasus::server
