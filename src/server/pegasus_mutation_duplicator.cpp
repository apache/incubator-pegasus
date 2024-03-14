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

#include "pegasus_mutation_duplicator.h"

#include <absl/strings/string_view.h>
#include <fmt/core.h>
#include <pegasus/error.h>
#include <sys/types.h>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "client_lib/pegasus_client_impl.h"
#include "common/duplication_common.h"
#include "duplication_internal_types.h"
#include "pegasus/client.h"
#include "pegasus_key_schema.h"
#include "pegasus_rpc_types.h"
#include "rrdb/rrdb.code.definition.h"
#include "rrdb/rrdb_types.h"
#include "runtime/message_utils.h"
#include "runtime/rpc/rpc_message.h"
#include "server/pegasus_write_service.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/chrono_literals.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/rand.h"

METRIC_DEFINE_counter(replica,
                      dup_shipped_successful_requests,
                      dsn::metric_unit::kRequests,
                      "The number of successful DUPLICATE requests sent from client");

METRIC_DEFINE_counter(replica,
                      dup_shipped_failed_requests,
                      dsn::metric_unit::kRequests,
                      "The number of failed DUPLICATE requests sent from client");

METRIC_DEFINE_counter(replica,
                      dup_retry_non_idempotent_duplicate_request,
                      dsn::metric_unit::kRequests,
                      "The number of retried non-idempotent DUPLICATE requests sent from client");

DSN_DECLARE_uint32(duplicate_log_batch_bytes);
DSN_DECLARE_bool(duplication_unsafe_allow_non_idempotent);

namespace dsn {
namespace replication {
struct replica_base;

/// static definition of mutation_duplicator::creator.
/*static*/ std::function<std::unique_ptr<mutation_duplicator>(
    replica_base *, absl::string_view, absl::string_view)>
    mutation_duplicator::creator =
        [](replica_base *r, absl::string_view remote, absl::string_view app) {
            return std::make_unique<pegasus::server::pegasus_mutation_duplicator>(r, remote, app);
        };

} // namespace replication
} // namespace dsn

namespace pegasus {
namespace server {

using namespace dsn::literals::chrono_literals;

/*extern*/ uint64_t get_hash_from_request(dsn::task_code tc, const dsn::blob &data)
{
    if (tc == dsn::apps::RPC_RRDB_RRDB_PUT) {
        dsn::apps::update_request thrift_request;
        dsn::from_blob_to_thrift(data, thrift_request);
        return pegasus_key_hash(thrift_request.key);
    }
    if (tc == dsn::apps::RPC_RRDB_RRDB_REMOVE) {
        dsn::blob raw_key;
        dsn::from_blob_to_thrift(data, raw_key);
        return pegasus_key_hash(raw_key);
    }
    if (tc == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
        dsn::apps::multi_put_request thrift_request;
        dsn::from_blob_to_thrift(data, thrift_request);
        return pegasus_hash_key_hash(thrift_request.hash_key);
    }
    if (tc == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
        dsn::apps::multi_remove_request thrift_request;
        dsn::from_blob_to_thrift(data, thrift_request);
        return pegasus_hash_key_hash(thrift_request.hash_key);
    }
    if (tc == dsn::apps::RPC_RRDB_RRDB_INCR) {
        dsn::apps::incr_request thrift_request;
        dsn::from_blob_to_thrift(data, thrift_request);
        return pegasus_hash_key_hash(thrift_request.key);
    }
    if (tc == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_SET) {
        dsn::apps::check_and_set_request thrift_request;
        dsn::from_blob_to_thrift(data, thrift_request);
        return pegasus_hash_key_hash(thrift_request.hash_key);
    }
    if (tc == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_MUTATE) {
        dsn::apps::check_and_mutate_request thrift_request;
        dsn::from_blob_to_thrift(data, thrift_request);
        return pegasus_hash_key_hash(thrift_request.hash_key);
    }

    LOG_FATAL("unexpected task code: {}", tc);
    __builtin_unreachable();
}

pegasus_mutation_duplicator::pegasus_mutation_duplicator(dsn::replication::replica_base *r,
                                                         absl::string_view remote_cluster,
                                                         absl::string_view app)
    : mutation_duplicator(r),
      _remote_cluster(remote_cluster),
      METRIC_VAR_INIT_replica(dup_shipped_successful_requests),
      METRIC_VAR_INIT_replica(dup_shipped_failed_requests)
{
    // initialize pegasus-client when this class is first time used.
    static __attribute__((unused)) bool _dummy = pegasus_client_factory::initialize(nullptr);

    pegasus_client *client = pegasus_client_factory::get_client(remote_cluster.data(), app.data());
    _client = static_cast<client::pegasus_client_impl *>(client);

    auto ret = dsn::replication::get_duplication_cluster_id(remote_cluster.data());
    CHECK_PREFIX_MSG(ret.is_ok(), // never possible, meta server disallows such remote_cluster.
                     "invalid remote cluster: {}, err_ret: {}",
                     remote_cluster,
                     ret.get_error());
    _remote_cluster_id = static_cast<uint8_t>(ret.get_value());

    LOG_INFO_PREFIX("initialize mutation duplicator for local cluster [id:{}], "
                    "remote cluster [id:{}, addr:{}]",
                    get_current_cluster_id(),
                    _remote_cluster_id,
                    remote_cluster);

    // never possible to duplicate data to itself
    CHECK_NE_PREFIX_MSG(
        get_current_cluster_id(), _remote_cluster_id, "invalid remote cluster: {}", remote_cluster);
}

void pegasus_mutation_duplicator::send(uint64_t hash, callback cb)
{
    duplicate_rpc rpc;
    {
        dsn::zauto_lock _(_lock);
        rpc = _inflights[hash].front();
        _inflights[hash].pop_front();
    }

    _client->async_duplicate(rpc,
                             [hash, cb, rpc, this](dsn::error_code err) mutable {
                                 on_duplicate_reply(hash, std::move(cb), std::move(rpc), err);
                             },
                             _env.__conf.tracker);
}

void pegasus_mutation_duplicator::on_duplicate_reply(uint64_t hash,
                                                     mutation_duplicator::callback cb,
                                                     duplicate_rpc rpc,
                                                     dsn::error_code err)
{
    int perr = PERR_OK;
    if (err == dsn::ERR_OK) {
        perr = client::pegasus_client_impl::get_client_error(
            client::pegasus_client_impl::get_rocksdb_server_error(rpc.response().error));
    }

    if (perr != PERR_OK || err != dsn::ERR_OK) {
        METRIC_VAR_INCREMENT(dup_shipped_failed_requests);

        // randomly log the 1% of the failed duplicate rpc, because minor number of
        // errors are acceptable.
        // TODO(wutao1): print the entire request for future debugging.
        if (dsn::rand::next_double01() <= 0.01) {
            LOG_ERROR_PREFIX("duplicate_rpc failed: {} [size:{}]",
                             err == dsn::ERR_OK ? _client->get_error_string(perr) : err.to_string(),
                             rpc.request().entries.size());
        }
        // duplicating an illegal write to server is unacceptable, fail fast.
        CHECK_NE_PREFIX_MSG(perr, PERR_INVALID_ARGUMENT, rpc.response().error_hint);
    } else {
        METRIC_VAR_INCREMENT(dup_shipped_successful_requests);
        _total_shipped_size +=
            rpc.dsn_request()->header->body_length + rpc.dsn_request()->header->hdr_length;
    }

    {
        dsn::zauto_lock _(_lock);
        if (perr != PERR_OK || err != dsn::ERR_OK) {
            // retry this rpc
            _inflights[hash].push_front(rpc);
            _env.schedule([hash, cb, this]() { send(hash, cb); }, 1_s);

            type_force_send_non_idempotent_if_need(rpc);

            return;
        }
        if (_inflights[hash].empty()) {
            _inflights.erase(hash);
            if (_inflights.empty()) {
                // move forward to the next step.
                cb(_total_shipped_size);
            }
        } else {
            // start next rpc immediately
            _env.schedule([hash, cb, this]() { send(hash, cb); });
            return;
        }
    }
}

void pegasus_mutation_duplicator::type_force_send_non_idempotent_if_need(duplicate_rpc &rpc)
{
    if (!FLAGS_duplication_unsafe_allow_non_idempotent) {
        return;
    }

    // there maybe more than one mutation in one dup rpc
    for (auto entry : rpc.request().entries) {
        // not a non idempotent request
        if (!_non_idempotent_codes.count(entry.task_code)) {
            continue;
        }

        METRIC_VAR_INCREMENT(dup_retry_non_idempotent_duplicate_request);
        dsn::message_ex *write = dsn::from_blob_to_received_msg(entry.task_code, entry.raw_message);

        if (entry.task_code == dsn::apps::RPC_RRDB_RRDB_INCR) {
            incr_rpc raw_rpc(write);

            LOG_DEBUG("Non-indempotent write RPC_RRDB_RRDB_INCR has been retried when doing "
                      "duplication, key is '{}'",
                      raw_rpc.request().key);
            continue;
        }

        if (entry.task_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_SET) {
            check_and_set_rpc raw_rpc(write);

            LOG_DEBUG("Non-indempotent write RPC_RRDB_RRDB_CHECK_AND_SET has been retried "
                      "when doing duplication,"
                      "hash key [{}], check sort key [{}],"
                      "set sort key [{}]",
                      raw_rpc.request().hash_key,
                      raw_rpc.request().check_sort_key,
                      raw_rpc.request().set_sort_key);
            continue;
        }

        if (entry.task_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_MUTATE) {
            check_and_mutate_rpc raw_rpc(write);

            LOG_DEBUG("Non-indempotent write RPC_RRDB_RRDB_CHECK_AND_MUTATE has been "
                      "retried when doing duplication,"
                      "hash key is [{}] , sort key is [{}] .",
                      raw_rpc.request().hash_key,
                      raw_rpc.request().check_sort_key);
            continue;
        }
    }
}

void pegasus_mutation_duplicator::duplicate(mutation_tuple_set muts, callback cb)
{
    _total_shipped_size = 0;

    auto batch_request = std::make_unique<dsn::apps::duplicate_request>();
    uint batch_count = 0;
    uint batch_bytes = 0;
    for (auto mut : muts) {
        // mut: 0=timestamp, 1=rpc_code, 2=raw_message
        batch_count++;
        dsn::task_code rpc_code = std::get<1>(mut);
        dsn::blob raw_message = std::get<2>(mut);
        auto dreq = std::make_unique<dsn::apps::duplicate_request>();

        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_DUPLICATE) {
            // ignore if it is a DUPLICATE
            // Because DUPLICATE comes from other clusters should not be forwarded to any other
            // destinations. A DUPLICATE is meant to be targeting only one cluster.
            continue;
        } else {
            dsn::apps::duplicate_entry entry;
            entry.__set_raw_message(raw_message);
            entry.__set_task_code(rpc_code);
            entry.__set_timestamp(std::get<0>(mut));
            entry.__set_cluster_id(get_current_cluster_id());
            batch_request->entries.emplace_back(std::move(entry));
            batch_bytes += raw_message.length();
        }

        if (batch_count == muts.size() || batch_bytes >= FLAGS_duplicate_log_batch_bytes) {
            // since all the plog's mutations of replica belong to same gpid though the hash of
            // mutation is different, use the last mutation of one batch to get and represents the
            // current hash value, it will still send to remote correct replica
            uint64_t hash = get_hash_from_request(rpc_code, raw_message);
            duplicate_rpc rpc(std::move(batch_request),
                              dsn::apps::RPC_RRDB_RRDB_DUPLICATE,
                              100_s, // TODO(wutao1): configurable timeout.
                              hash);
            _inflights[hash].push_back(std::move(rpc));
            batch_request = std::make_unique<dsn::apps::duplicate_request>();
            batch_bytes = 0;
        }
    }

    if (_inflights.empty()) {
        cb(0);
        return;
    }
    auto inflights = _inflights;
    for (const auto &kv : inflights) {
        send(kv.first, cb);
    }
}

} // namespace server
} // namespace pegasus
