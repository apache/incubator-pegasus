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

#include <fmt/core.h>
#include <rocksdb/status.h>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bulk_load/replica_bulk_loader.h"
#include "bulk_load_types.h"
#include "common/fs_manager.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "common/replication_common.h"
#include "common/replication_enums.h"
#include "common/replication_other_types.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "metadata_types.h"
#include "mutation.h"
#include "mutation_log.h"
#include "ranger/access_type.h"
#include "replica.h"
#include "replica/prepare_list.h"
#include "replica/replica_context.h"
#include "replica/replication_app_base.h"
#include "replica_stub.h"
#include "rpc/dns_resolver.h"
#include "rpc/rpc_host_port.h"
#include "rpc/rpc_message.h"
#include "rpc/rpc_stream.h"
#include "rpc/serialization.h"
#include "runtime/api_layer1.h"
#include "security/access_controller.h"
#include "split/replica_split_manager.h"
#include "task/async_calls.h"
#include "task/task.h"
#include "task/task_code.h"
#include "task/task_spec.h"
#include "utils/api_utilities.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/latency_tracer.h"
#include "utils/metrics.h"
#include "utils/ports.h"
#include "utils/thread_access_checker.h"
#include "utils/uniq_timestamp_us.h"

DSN_DEFINE_bool(replication,
                reject_write_when_disk_insufficient,
                true,
                "reject client write requests if disk status is space insufficient");
DSN_TAG_VARIABLE(reject_write_when_disk_insufficient, FT_MUTABLE);

DSN_DEFINE_int32(replication,
                 prepare_timeout_ms_for_secondaries,
                 3000,
                 "The timeout in millisecond for the primary replicas to send prepare requests to "
                 "the secondaries in two phase commit");
DSN_DEFINE_int32(replication,
                 prepare_timeout_ms_for_potential_secondaries,
                 5000,
                 "The timeout in millisecond for the primary replicas to send prepare requests to "
                 "the learners in two phase commit");
DSN_DEFINE_int32(replication,
                 prepare_decree_gap_for_debug_logging,
                 10000,
                 "if greater than 0, then print debug log every decree gap of preparing");
DSN_DEFINE_uint64(
    replication,
    max_allowed_write_size,
    1 << 20,
    "write operation exceed this threshold will be logged and reject, 0 means not check");

DSN_DECLARE_int32(max_mutation_count_in_prepare_list);
DSN_DECLARE_int32(staleness_for_commit);

namespace dsn {
namespace replication {
void replica::on_client_write(dsn::message_ex *request, bool ignore_throttling)
{
    _checker.only_one_thread_access();

    if (!_access_controller->allowed(request, ranger::access_type::kWrite)) {
        response_client_write(request, ERR_ACL_DENY);
        return;
    }

    if (_deny_client.write) {
        if (_deny_client.reconfig) {
            // return ERR_INVALID_STATE will trigger client update config immediately
            response_client_write(request, ERR_INVALID_STATE);
            return;
        }
        // Do not reply any message to the peer client to let it timeout, it's OK coz some users
        // may retry immediately when they got a not success code which will make the server side
        // pressure more and more heavy.
        return;
    }

    if (dsn_unlikely(FLAGS_max_allowed_write_size > 0 &&
                     request->body_size() > FLAGS_max_allowed_write_size)) {
        std::string request_info = _app->dump_write_request(request);
        LOG_WARNING_PREFIX(
            "client from {} write request body size exceed threshold, request = [{}], "
            "request_body_size "
            "= {}, FLAGS_max_allowed_write_size = {}, it will be rejected!",
            request->header->from_address,
            request_info,
            request->body_size(),
            FLAGS_max_allowed_write_size);
        METRIC_VAR_INCREMENT(write_size_exceed_threshold_requests);
        response_client_write(request, ERR_INVALID_DATA);
        return;
    }

    if (dsn_unlikely(request->rpc_code() == TASK_CODE_INVALID)) {
        LOG_ERROR("recv message with invalid RPC code {} from {}, trace_id = {}",
                  request->rpc_code(),
                  request->header->from_address,
                  request->header->trace_id);
        response_client_write(request, ERR_INVALID_PARAMETERS);
        return;
    }

    auto *spec = task_spec::get(request->rpc_code());
    if (dsn_unlikely(spec == nullptr)) {
        LOG_ERROR("recv message with unhandled RPC code {} from {}, trace_id = {}",
                  request->rpc_code(),
                  request->header->from_address,
                  request->header->trace_id);
        response_client_write(request, ERR_HANDLER_NOT_FOUND);
        return;
    }

    if (need_reject_non_idempotent(spec)) {
        // Ignore non-idempotent write, because duplication provides no guarantee of atomicity to
        // make this write produce the same result on multiple clusters.
        METRIC_VAR_INCREMENT(dup_rejected_non_idempotent_write_requests);
        response_client_write(request, ERR_OPERATION_DISABLED);
        return;
    }

    CHECK_REQUEST_IF_SPLITTING(write);

    if (partition_status::PS_PRIMARY != status()) {
        response_client_write(request, ERR_INVALID_STATE);
        return;
    }

    if (FLAGS_reject_write_when_disk_insufficient &&
        (_dir_node->status != disk_status::NORMAL || _primary_states.secondary_disk_abnormal())) {
        response_client_write(request, disk_status_to_error_code(_dir_node->status));
        return;
    }

    if (_is_bulk_load_ingestion) {
        if (request->rpc_code() != dsn::apps::RPC_RRDB_RRDB_BULK_LOAD) {
            // reject write requests during ingestion
            METRIC_VAR_INCREMENT(bulk_load_ingestion_rejected_write_requests);
            response_client_write(request, ERR_OPERATION_DISABLED);
        } else {
            response_client_write(request, ERR_NO_NEED_OPERATE);
        }
        return;
    }

    if (static_cast<int32_t>(_primary_states.pc.hp_secondaries.size()) + 1 <
        _options->app_mutation_2pc_min_replica_count(_app_info.max_replica_count)) {
        response_client_write(request, ERR_NOT_ENOUGH_MEMBER);
        return;
    }

    if (!ignore_throttling && throttle_write_request(request)) {
        return;
    }

    if (request->rpc_code() == dsn::apps::RPC_RRDB_RRDB_BULK_LOAD) {
        auto cur_bulk_load_status = _bulk_loader->get_bulk_load_status();
        if (cur_bulk_load_status != bulk_load_status::BLS_DOWNLOADED &&
            cur_bulk_load_status != bulk_load_status::BLS_INGESTING) {
            LOG_ERROR_PREFIX("receive bulk load ingestion request with wrong status({})",
                             enum_to_string(cur_bulk_load_status));
            response_client_write(request, ERR_INVALID_STATE);
            return;
        }
        LOG_INFO_PREFIX("receive bulk load ingestion request");

        // bulk load ingestion request requires that all secondaries should be alive
        if (static_cast<int32_t>(_primary_states.pc.hp_secondaries.size()) + 1 <
            _primary_states.pc.max_replica_count) {
            response_client_write(request, ERR_NOT_ENOUGH_MEMBER);
            return;
        }
        _is_bulk_load_ingestion = true;
        _bulk_load_ingestion_start_time_ms = dsn_now_ms();
    }

    LOG_DEBUG_PREFIX("got write request from {}", request->header->from_address);
    auto mu = _primary_states.write_queue.add_work(request);
    if (mu != nullptr) {
        init_prepare(mu, false);
    }
}

bool replica::need_reject_non_idempotent(task_spec *spec) const
{
    if (!is_duplication_master()) {
        return false;
    }

    if (_make_write_idempotent) {
        return false;
    }

    return !spec->rpc_request_is_write_idempotent;
}

bool replica::need_make_idempotent(task_spec *spec) const
{
    if (!_make_write_idempotent) {
        return false;
    }

    return !spec->rpc_request_is_write_idempotent;
}

bool replica::need_make_idempotent(message_ex *request) const
{
    if (request == nullptr) {
        return false;
    }

    if (!_make_write_idempotent) {
        return false;
    }

    auto *spec = task_spec::get(request->rpc_code());
    CHECK_NOTNULL(spec, "RPC code {} not found", request->rpc_code());

    return !spec->rpc_request_is_write_idempotent;
}

int replica::make_idempotent(mutation_ptr &mu)
{
    CHECK_TRUE(!mu->client_requests.empty());

    message_ex *request = mu->client_requests.front();
    if (!need_make_idempotent(request)) {
        return rocksdb::Status::kOk;
    }

    // The original atomic write request must not be batched.
    CHECK_EQ(mu->client_requests.size(), 1);

    dsn::message_ex *new_request = nullptr;
    const int err = _app->make_idempotent(request, &new_request);
    if (dsn_unlikely(err != rocksdb::Status::kOk)) {
        // Once some error occurred, the response with error must have been returned to the
        // client during _app->make_idempotent(). Thus do nothing here.
        return err;
    }

    CHECK_NOTNULL(new_request,
                  "new_request should not be null since its original write request must be atomic");

    // During make_idempotent(), the request has been deserialized (i.e. unmarshall() in the
    // constructor of `rpc_holder::internal`). Once deserialize it again, assertion would fail for
    // set_read_msg() in the constructor of `rpc_read_stream`.
    //
    // To make it deserializable again to be applied into RocksDB, restore read for it.
    request->restore_read();

    // The decree must have not been assigned.
    CHECK_EQ(mu->get_decree(), invalid_decree);

    // Create a new mutation to hold the new idempotent request. The old mutation holding the
    // original atomic write request will be released automatically.
    mu = new_mutation(invalid_decree, request);
    mu->add_client_request(new_request);
    return rocksdb::Status::kOk;
}

void replica::init_prepare(mutation_ptr &mu, bool reconciliation, bool pop_all_committed_mutations)
{
    CHECK_EQ(partition_status::PS_PRIMARY, status());

    if (make_idempotent(mu) != rocksdb::Status::kOk) {
        // If some error occurred, the response with error must have been returned to the
        // client during make_idempotent(). Thus do nothing here.
        return;
    }

    mu->_tracer->set_description("primary");
    ADD_POINT(mu->_tracer);

    error_code err = ERR_OK;
    uint8_t count = 0;
    const auto request_count = mu->client_requests.size();
    mu->data.header.last_committed_decree = last_committed_decree();

    log_level_t level = LOG_LEVEL_DEBUG;
    if (mu->data.header.decree == invalid_decree) {
        mu->set_id(get_ballot(), _prepare_list->max_decree() + 1);
        // print a debug log if necessary
        if (FLAGS_prepare_decree_gap_for_debug_logging > 0 &&
            mu->get_decree() % FLAGS_prepare_decree_gap_for_debug_logging == 0) {
            level = LOG_LEVEL_INFO;
        }
        mu->set_timestamp(static_cast<int64_t>(_uniq_timestamp_us.next()));
    } else {
        mu->set_id(get_ballot(), mu->data.header.decree);
    }

    mu->_tracer->set_name(fmt::format("mutation[{}]", mu->name()));
    LOG(level, "{}: mutation {} init_prepare, mutation_tid={}", name(), mu->name(), mu->tid());

    // child should prepare mutation synchronously
    mu->set_is_sync_to_child(_primary_states.sync_send_write_request);

    // check bounded staleness
    if (mu->data.header.decree > last_committed_decree() + FLAGS_staleness_for_commit) {
        err = ERR_CAPACITY_EXCEEDED;
        reply_with_error(mu, err);
        return;
    }

    // stop prepare bulk load ingestion if there are secondaries unalive
    for (auto i = 0; i < request_count; ++i) {
        const mutation_update &update = mu->data.updates[i];
        if (update.code != dsn::apps::RPC_RRDB_RRDB_BULK_LOAD) {
            break;
        }
        LOG_INFO_PREFIX("try to prepare bulk load mutation({})", mu->name());
        if (static_cast<int32_t>(_primary_states.pc.hp_secondaries.size()) + 1 <
            _primary_states.pc.max_replica_count) {
            err = ERR_NOT_ENOUGH_MEMBER;
            break;
        }
    }
    if (err != ERR_OK) {
        reply_with_error(mu, err);
        return;
    }

    // stop prepare if there are too few replicas unless it's a reconciliation
    // for reconciliation, we should ensure every prepared mutation to be committed
    // please refer to PacificA paper
    if (static_cast<int32_t>(_primary_states.pc.hp_secondaries.size()) + 1 <
            _options->app_mutation_2pc_min_replica_count(_app_info.max_replica_count) &&
        !reconciliation) {
        err = ERR_NOT_ENOUGH_MEMBER;
        reply_with_error(mu, err);
        return;
    }

    CHECK_GT(mu->data.header.decree, last_committed_decree());

    // local prepare
    err = _prepare_list->prepare(mu, partition_status::PS_PRIMARY, pop_all_committed_mutations);
    if (err != ERR_OK) {
        reply_with_error(mu, err);
        return;
    }

    // remote prepare
    mu->set_prepare_ts();
    mu->set_left_secondary_ack_count(
        static_cast<unsigned int>(_primary_states.pc.hp_secondaries.size()));
    for (const auto &secondary : _primary_states.pc.hp_secondaries) {
        send_prepare_message(secondary,
                             partition_status::PS_SECONDARY,
                             mu,
                             FLAGS_prepare_timeout_ms_for_secondaries,
                             pop_all_committed_mutations,
                             invalid_signature);
    }

    count = 0;
    for (auto it = _primary_states.learners.begin(); it != _primary_states.learners.end(); ++it) {
        if (it->second.prepare_start_decree != invalid_decree &&
            mu->data.header.decree >= it->second.prepare_start_decree) {
            send_prepare_message(it->first,
                                 partition_status::PS_POTENTIAL_SECONDARY,
                                 mu,
                                 FLAGS_prepare_timeout_ms_for_potential_secondaries,
                                 pop_all_committed_mutations,
                                 it->second.signature);
            count++;
        }
    }
    mu->set_left_potential_secondary_ack_count(count);

    if (_split_mgr->is_splitting()) {
        _split_mgr->copy_mutation(mu);
    }

    if (mu->is_logged()) {
        do_possible_commit_on_primary(mu);
    } else {
        CHECK_EQ(mu->data.header.log_offset, invalid_offset);
        CHECK(mu->log_task() == nullptr, "");
        int64_t pending_size;
        mu->log_task() = _private_log->append(mu,
                                              LPC_WRITE_REPLICATION_LOG,
                                              &_tracker,
                                              std::bind(&replica::on_append_log_completed,
                                                        this,
                                                        mu,
                                                        std::placeholders::_1,
                                                        std::placeholders::_2),
                                              get_gpid().thread_hash(),
                                              &pending_size);
        CHECK_NOTNULL(mu->log_task(), "");
    }

    _primary_states.last_prepare_ts_ms = mu->prepare_ts_ms();
}

void replica::reply_with_error(const mutation_ptr &mu, const error_code &err)
{
    // Respond to the original atomic request if it is non-null. And it could never be batched.
    if (mu->original_request != nullptr) {
        response_client_write(mu->original_request, err);
        return;
    }

    // Just respond to each client request directly if there is no original request for them.
    for (auto *req : mu->client_requests) {
        response_client_write(req, err);
    }
}

void replica::send_prepare_message(const host_port &hp,
                                   partition_status::type status,
                                   const mutation_ptr &mu,
                                   int timeout_milliseconds,
                                   bool pop_all_committed_mutations,
                                   int64_t learn_signature)
{
    mu->_tracer->add_sub_tracer(hp.to_string());
    ADD_POINT(mu->_tracer->sub_tracer(hp.to_string()));

    dsn::message_ex *msg = dsn::message_ex::create_request(
        RPC_PREPARE, timeout_milliseconds, get_gpid().thread_hash());
    replica_configuration rconfig;
    _primary_states.get_replica_config(status, rconfig, learn_signature);
    rconfig.__set_pop_all(pop_all_committed_mutations);
    if (status == partition_status::PS_SECONDARY && _primary_states.sync_send_write_request) {
        rconfig.__set_split_sync_to_child(true);
    }

    {
        rpc_write_stream writer(msg);
        marshall(writer, get_gpid(), DSF_THRIFT_BINARY);
        marshall(writer, rconfig, DSF_THRIFT_BINARY);
        mu->write_to(writer, msg);
    }

    mu->remote_tasks()[hp] = rpc::call(
        dsn::dns_resolver::instance().resolve_address(hp),
        msg,
        &_tracker,
        [=](error_code err, dsn::message_ex *request, dsn::message_ex *reply) {
            on_prepare_reply(std::make_pair(mu, rconfig.status), err, request, reply);
        },
        get_gpid().thread_hash());

    LOG_DEBUG_PREFIX("mutation {} send_prepare_message to {} as {}",
                     mu->name(),
                     hp,
                     enum_to_string(rconfig.status));
}

void replica::do_possible_commit_on_primary(mutation_ptr &mu)
{
    CHECK_EQ(_config.ballot, mu->data.header.ballot);
    CHECK_EQ(partition_status::PS_PRIMARY, status());

    if (mu->is_ready_for_commit()) {
        _prepare_list->commit(mu->data.header.decree, COMMIT_ALL_READY);
    }
}

void replica::on_prepare(dsn::message_ex *request)
{
    _checker.only_one_thread_access();

    replica_configuration rconfig;
    mutation_ptr mu;
    bool pop_all_committed_mutations = false;

    {
        rpc_read_stream reader(request);
        unmarshall(reader, rconfig, DSF_THRIFT_BINARY);
        mu = mutation::read_from(reader, request);
        mu->set_is_sync_to_child(rconfig.split_sync_to_child);
        pop_all_committed_mutations = rconfig.pop_all;
        rconfig.split_sync_to_child = false;
        rconfig.pop_all = false;
    }

    decree decree = mu->data.header.decree;

    LOG_DEBUG_PREFIX("mutation {} on_prepare", mu->name());
    mu->_tracer->set_name(fmt::format("mutation[{}]", mu->name()));
    mu->_tracer->set_description("secondary");
    ADD_POINT(mu->_tracer);

    CHECK_EQ(mu->data.header.pid, rconfig.pid);
    CHECK_EQ(mu->data.header.ballot, rconfig.ballot);

    if (mu->data.header.ballot < get_ballot()) {
        LOG_ERROR_PREFIX("mutation {} on_prepare skipped due to old view", mu->name());
        // no need response because the rpc should have been cancelled on primary in this case
        return;
    }

    // update configuration when necessary
    else if (rconfig.ballot > get_ballot()) {
        if (!update_local_configuration(rconfig)) {
            LOG_ERROR_PREFIX(
                "mutation {} on_prepare failed as update local configuration failed, state = {}",
                mu->name(),
                enum_to_string(status()));
            ack_prepare_message(ERR_INVALID_STATE, mu);
            return;
        }
    }

    if (partition_status::PS_INACTIVE == status() || partition_status::PS_ERROR == status()) {
        LOG_ERROR_PREFIX("mutation {} on_prepare failed as invalid replica state, state = {}",
                         mu->name(),
                         enum_to_string(status()));
        ack_prepare_message((partition_status::PS_INACTIVE == status() && _inactive_is_transient)
                                ? ERR_INACTIVE_STATE
                                : ERR_INVALID_STATE,
                            mu);
        return;
    } else if (partition_status::PS_POTENTIAL_SECONDARY == status()) {
        // new learning process
        if (rconfig.learner_signature != _potential_secondary_states.learning_version) {
            LOG_ERROR_PREFIX("mutation {} on_prepare failed as unmatched learning signature, state "
                             "= {}, old_signature[{:#018x}] vs new_signature[{:#018x}]",
                             mu->name(),
                             enum_to_string(status()),
                             _potential_secondary_states.learning_version,
                             rconfig.learner_signature);
            handle_learning_error(ERR_INVALID_STATE, false);
            ack_prepare_message(ERR_INVALID_STATE, mu);
            return;
        }

        auto learning_status = _potential_secondary_states.learning_status;
        if (learning_status != learner_status::LearningWithPrepare &&
            learning_status != learner_status::LearningSucceeded) {
            // if prepare requests are received when learning status is changing from
            // LearningWithoutPrepare to LearningWithPrepare, we ack ERR_TRY_AGAIN.
            error_code ack_code =
                (learning_status == learner_status::LearningWithoutPrepare ? ERR_TRY_AGAIN
                                                                           : ERR_INVALID_STATE);
            LOG_ERROR_PREFIX(
                "mutation {} on_prepare skipped as invalid learning status, state = {}, "
                "learning_status = {}, ack {}",
                mu->name(),
                enum_to_string(status()),
                enum_to_string(learning_status),
                ack_code);
            ack_prepare_message(ack_code, mu);
            return;
        }
    }

    CHECK_EQ(rconfig.status, status());
    if (decree <= last_committed_decree()) {
        ack_prepare_message(ERR_OK, mu);
        return;
    }

    // real prepare start
    _uniq_timestamp_us.try_update(mu->data.header.timestamp);
    auto mu2 = _prepare_list->get_mutation_by_decree(decree);
    if (mu2 != nullptr && mu2->data.header.ballot == mu->data.header.ballot) {
        if (mu2->is_logged()) {
            // already logged, just response ERR_OK
            ack_prepare_message(ERR_OK, mu);
        } else {
            // not logged, combine duplicate request to old mutation
            mu2->add_prepare_request(request);
        }
        return;
    }

    error_code err = _prepare_list->prepare(mu, status(), pop_all_committed_mutations);
    CHECK_EQ_MSG(err, ERR_OK, "prepare mutation failed");

    if (partition_status::PS_POTENTIAL_SECONDARY == status() ||
        partition_status::PS_SECONDARY == status()) {
        CHECK_LE_MSG(mu->data.header.decree,
                     last_committed_decree() + FLAGS_max_mutation_count_in_prepare_list,
                     "last_committed_decree: {}, FLAGS_max_mutation_count_in_prepare_list: {}",
                     last_committed_decree(),
                     FLAGS_max_mutation_count_in_prepare_list);
    } else {
        LOG_ERROR_PREFIX("mutation {} on_prepare failed as invalid replica state, state = {}",
                         mu->name(),
                         enum_to_string(status()));
        ack_prepare_message(ERR_INVALID_STATE, mu);
        return;
    }

    if (_split_mgr->is_splitting()) {
        _split_mgr->copy_mutation(mu);
    }

    CHECK(mu->log_task() == nullptr, "");
    mu->log_task() = _private_log->append(mu,
                                          LPC_WRITE_REPLICATION_LOG,
                                          &_tracker,
                                          std::bind(&replica::on_append_log_completed,
                                                    this,
                                                    mu,
                                                    std::placeholders::_1,
                                                    std::placeholders::_2),
                                          get_gpid().thread_hash());
    CHECK_NOTNULL(mu->log_task(), "");
}

void replica::on_append_log_completed(mutation_ptr &mu, error_code err, size_t size)
{
    _checker.only_one_thread_access();

    LOG_DEBUG_PREFIX(
        "append shared log completed for mutation {}, size = {}, err = {}", mu->name(), size, err);

    ADD_POINT(mu->_tracer);

    if (err == ERR_OK) {
        mu->set_logged();
    } else {
        LOG_ERROR_PREFIX("append shared log failed for mutation {}, err = {}", mu->name(), err);
    }

    // skip old mutations
    if (mu->data.header.ballot >= get_ballot() && status() != partition_status::PS_INACTIVE) {
        switch (status()) {
        case partition_status::PS_PRIMARY:
            if (err == ERR_OK) {
                do_possible_commit_on_primary(mu);
            } else {
                handle_local_failure(err);
            }
            break;
        case partition_status::PS_SECONDARY:
        case partition_status::PS_POTENTIAL_SECONDARY:
            if (err != ERR_OK) {
                handle_local_failure(err);
            }
            // always ack
            ack_prepare_message(err, mu);
            // all mutations with lower decree must be ready
            _prepare_list->commit(mu->data.header.last_committed_decree, COMMIT_TO_DECREE_HARD);
            break;
        case partition_status::PS_PARTITION_SPLIT:
            if (err != ERR_OK) {
                handle_local_failure(err);
            }
            _split_mgr->ack_parent(err, mu);
            break;
        case partition_status::PS_ERROR:
            break;
        default:
            CHECK(false, "invalid partition_status, status = {}", enum_to_string(status()));
            break;
        }
    }

    if (err != ERR_OK) {
        // mutation log failure, propagate to all replicas
        _stub->handle_log_failure(err);
    }
}

void replica::on_prepare_reply(std::pair<mutation_ptr, partition_status::type> pr,
                               error_code err,
                               dsn::message_ex *request,
                               dsn::message_ex *reply)
{
    _checker.only_one_thread_access();

    mutation_ptr mu = pr.first;
    partition_status::type target_status = pr.second;

    // skip callback for old mutations
    if (partition_status::PS_PRIMARY != status() || mu->data.header.ballot < get_ballot() ||
        mu->get_decree() <= last_committed_decree())
        return;

    CHECK_EQ_MSG(mu->data.header.ballot, get_ballot(), "{}: invalid mutation ballot", mu->name());

    const auto &node = request->to_host_port;
    partition_status::type st = _primary_states.get_node_status(node);

    // handle reply
    prepare_ack resp;

    // handle error
    if (err != ERR_OK) {
        resp.err = err;
    } else {
        ::dsn::unmarshall(reply, resp);
    }

    auto send_prepare_tracer = mu->_tracer->sub_tracer(request->to_host_port.to_string());
    APPEND_EXTERN_POINT(send_prepare_tracer, resp.receive_timestamp, "remote_receive");
    APPEND_EXTERN_POINT(send_prepare_tracer, resp.response_timestamp, "remote_reply");
    ADD_CUSTOM_POINT(send_prepare_tracer, resp.err.to_string());

    if (resp.err == ERR_OK) {
        LOG_DEBUG_PREFIX("mutation {} on_prepare_reply from {}, appro_data_bytes = {}, "
                         "target_status = {}, err = {}",
                         mu->name(),
                         node,
                         mu->appro_data_bytes(),
                         enum_to_string(target_status),
                         resp.err);
    } else {
        LOG_ERROR_PREFIX("mutation {} on_prepare_reply from {}, appro_data_bytes = {}, "
                         "target_status = {}, err = {}",
                         mu->name(),
                         node,
                         mu->appro_data_bytes(),
                         enum_to_string(target_status),
                         resp.err);
    }

    if (resp.err == ERR_OK) {
        CHECK_EQ(resp.ballot, get_ballot());
        CHECK_EQ(resp.decree, mu->data.header.decree);

        switch (target_status) {
        case partition_status::PS_SECONDARY:
            CHECK(_primary_states.check_exist(node, partition_status::PS_SECONDARY),
                  "invalid secondary node address, address = {}",
                  node);
            CHECK_GT(mu->left_secondary_ack_count(), 0);
            if (0 == mu->decrease_left_secondary_ack_count()) {
                do_possible_commit_on_primary(mu);
            }
            break;
        case partition_status::PS_POTENTIAL_SECONDARY:
            CHECK_GT(mu->left_potential_secondary_ack_count(), 0);
            if (0 == mu->decrease_left_potential_secondary_ack_count()) {
                do_possible_commit_on_primary(mu);
            }
            break;
        default:
            LOG_WARNING_PREFIX("mutation {} prepare ack skipped coz the node is now inactive",
                               mu->name());
            break;
        }
    }

    // failure handling
    else {
        // retry for INACTIVE or TRY_AGAIN if there is still time.
        if (resp.err == ERR_INACTIVE_STATE || resp.err == ERR_TRY_AGAIN) {
            int prepare_timeout_ms = (target_status == partition_status::PS_SECONDARY
                                          ? FLAGS_prepare_timeout_ms_for_secondaries
                                          : FLAGS_prepare_timeout_ms_for_potential_secondaries);
            int delay_time_ms = 5; // delay some time before retry to avoid sending too frequently
            if (mu->is_prepare_close_to_timeout(delay_time_ms + 2, prepare_timeout_ms)) {
                LOG_ERROR_PREFIX("mutation {} do not retry prepare to {} for no enought time left, "
                                 "prepare_ts_ms = {}, prepare_timeout_ms = {}, now_ms = {}",
                                 mu->name(),
                                 node,
                                 mu->prepare_ts_ms(),
                                 prepare_timeout_ms,
                                 dsn_now_ms());
            } else {
                LOG_INFO_PREFIX(
                    "mutation {} retry prepare to {} after {} ms", mu->name(), node, delay_time_ms);
                int64_t learn_signature = invalid_signature;
                if (target_status == partition_status::PS_POTENTIAL_SECONDARY) {
                    auto it = _primary_states.learners.find(node);
                    if (it != _primary_states.learners.end()) {
                        learn_signature = it->second.signature;
                    }
                }
                tasking::enqueue(
                    LPC_DELAY_PREPARE,
                    &_tracker,
                    [this, node, target_status, mu, prepare_timeout_ms, learn_signature] {
                        // need to check status/ballot/decree before sending prepare message,
                        // because the config may have been changed or the mutation may have been
                        // committed during the delay time.
                        if (status() == partition_status::PS_PRIMARY &&
                            get_ballot() == mu->data.header.ballot &&
                            mu->get_decree() > last_committed_decree()) {
                            send_prepare_message(node,
                                                 target_status,
                                                 mu,
                                                 prepare_timeout_ms,
                                                 false,
                                                 learn_signature);
                        }
                    },
                    get_gpid().thread_hash(),
                    std::chrono::milliseconds(delay_time_ms));
                return;
            }
        }

        METRIC_VAR_INCREMENT(prepare_failed_requests);

        // make sure this is before any later commit ops
        // because now commit ops may lead to new prepare ops
        // due to replication throttling
        handle_remote_failure(st, node, resp.err, "prepare");

        // note targetStatus and (curent) status may diff
        if (target_status == partition_status::PS_POTENTIAL_SECONDARY) {
            CHECK_GT(mu->left_potential_secondary_ack_count(), 0);
            if (0 == mu->decrease_left_potential_secondary_ack_count()) {
                do_possible_commit_on_primary(mu);
            }
        }
    }
}

void replica::ack_prepare_message(error_code err, mutation_ptr &mu)
{
    ADD_POINT(mu->_tracer);
    prepare_ack resp;
    resp.pid = get_gpid();
    resp.err = err;
    resp.ballot = get_ballot();
    resp.decree = mu->data.header.decree;

    resp.__set_receive_timestamp(mu->_tracer->start_time());
    resp.__set_response_timestamp(dsn_now_ns());

    // for partition_status::PS_POTENTIAL_SECONDARY ONLY
    resp.last_committed_decree_in_app = _app->last_committed_decree();
    resp.last_committed_decree_in_prepare_list = last_committed_decree();

    const std::vector<dsn::message_ex *> &prepare_requests = mu->prepare_requests();
    CHECK(!prepare_requests.empty(), "mutation = {}", mu->name());

    if (err == ERR_OK) {
        if (mu->is_child_acked()) {
            LOG_DEBUG_PREFIX("mutation {} ack_prepare_message, err = {}", mu->name(), err);
            for (auto &request : prepare_requests) {
                reply(request, resp);
            }
        }
        return;
    }
    // only happened when prepare failed during partition split child copy mutation synchronously
    if (mu->is_error_acked()) {
        LOG_WARNING_PREFIX("mutation {} has been ack_prepare_message, err = {}", mu->name(), err);
        return;
    }

    LOG_WARNING_PREFIX("mutation {} ack_prepare_message, err = {}", mu->name(), err);
    if (mu->is_sync_to_child()) {
        mu->set_error_acked();
    }
    for (auto &request : prepare_requests) {
        reply(request, resp);
    }
}

void replica::cleanup_preparing_mutations(bool wait)
{
    decree start = last_committed_decree() + 1;
    decree end = _prepare_list->max_decree();

    for (decree decree = start; decree <= end; decree++) {
        mutation_ptr mu = _prepare_list->get_mutation_by_decree(decree);
        if (mu != nullptr) {
            mu->clear_prepare_or_commit_tasks();

            //
            // make sure the buffers from mutations are valid for underlying aio
            //
            if (wait) {
                if (dsn_unlikely(_private_log != nullptr)) {
                    _private_log->flush();
                }
                mu->wait_log_task();
            }
        }
    }
}
} // namespace replication
} // namespace dsn
