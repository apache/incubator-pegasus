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

#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include "bulk_load/replica_bulk_loader.h"
#include "split/replica_split_manager.h"
#include "runtime/security/access_controller.h"
#include <dsn/utils/latency_tracer.h>
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/dist/fmt_logging.h>

namespace dsn {
namespace replication {

DSN_DEFINE_bool("replication",
                reject_write_when_disk_insufficient,
                true,
                "reject client write requests if disk status is space insufficient");
DSN_TAG_VARIABLE(reject_write_when_disk_insufficient, FT_MUTABLE);

void replica::on_client_write(dsn::message_ex *request, bool ignore_throttling)
{
    _checker.only_one_thread_access();

    if (!_access_controller->allowed(request)) {
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

    if (dsn_unlikely(_stub->_max_allowed_write_size &&
                     request->body_size() > _stub->_max_allowed_write_size)) {
        std::string request_info = _app->dump_write_request(request);
        dwarn_replica("client from {} write request body size exceed threshold, request = [{}], "
                      "request_body_size "
                      "= {}, max_allowed_write_size = {}, it will be rejected!",
                      request->header->from_address.to_string(),
                      request_info,
                      request->body_size(),
                      _stub->_max_allowed_write_size);
        _stub->_counter_recent_write_size_exceed_threshold_count->increment();
        response_client_write(request, ERR_INVALID_DATA);
        return;
    }

    task_spec *spec = task_spec::get(request->rpc_code());
    if (dsn_unlikely(nullptr == spec || request->rpc_code() == TASK_CODE_INVALID)) {
        derror_f("recv message with unhandled rpc name {} from {}, trace_id = {}",
                 request->rpc_code().to_string(),
                 request->header->from_address.to_string(),
                 request->header->trace_id);
        response_client_write(request, ERR_HANDLER_NOT_FOUND);
        return;
    }

    if (is_duplication_master() && !spec->rpc_request_is_write_idempotent) {
        // Ignore non-idempotent write, because duplication provides no guarantee of atomicity to
        // make this write produce the same result on multiple clusters.
        _counter_dup_disabled_non_idempotent_write_count->increment();
        response_client_write(request, ERR_OPERATION_DISABLED);
        return;
    }

    CHECK_REQUEST_IF_SPLITTING(write)

    if (partition_status::PS_PRIMARY != status()) {
        response_client_write(request, ERR_INVALID_STATE);
        return;
    }

    if (FLAGS_reject_write_when_disk_insufficient &&
        (disk_space_insufficient() || _primary_states.secondary_disk_space_insufficient())) {
        response_client_write(request, ERR_DISK_INSUFFICIENT);
        return;
    }

    if (_is_bulk_load_ingestion) {
        if (request->rpc_code() != dsn::apps::RPC_RRDB_RRDB_BULK_LOAD) {
            // reject write requests during ingestion
            _counter_recent_write_bulk_load_ingestion_reject_count->increment();
            response_client_write(request, ERR_OPERATION_DISABLED);
        } else {
            response_client_write(request, ERR_NO_NEED_OPERATE);
        }
        return;
    }

    if (request->rpc_code() == dsn::apps::RPC_RRDB_RRDB_BULK_LOAD) {
        auto cur_bulk_load_status = _bulk_loader->get_bulk_load_status();
        if (cur_bulk_load_status != bulk_load_status::BLS_DOWNLOADED &&
            cur_bulk_load_status != bulk_load_status::BLS_INGESTING) {
            derror_replica("receive bulk load ingestion request with wrong status({})",
                           enum_to_string(cur_bulk_load_status));
            response_client_write(request, ERR_INVALID_STATE);
            return;
        }
        ddebug_replica("receive bulk load ingestion request");

        // bulk load ingestion request requires that all secondaries should be alive
        if (static_cast<int>(_primary_states.membership.secondaries.size()) + 1 <
            _primary_states.membership.max_replica_count) {
            response_client_write(request, ERR_NOT_ENOUGH_MEMBER);
            return;
        }
        _is_bulk_load_ingestion = true;
        _bulk_load_ingestion_start_time_ms = dsn_now_ms();
    }

    if (static_cast<int>(_primary_states.membership.secondaries.size()) + 1 <
        _options->app_mutation_2pc_min_replica_count(_app_info.max_replica_count)) {
        response_client_write(request, ERR_NOT_ENOUGH_MEMBER);
        return;
    }

    if (!ignore_throttling && throttle_write_request(request)) {
        return;
    }

    dinfo("%s: got write request from %s", name(), request->header->from_address.to_string());
    auto mu = _primary_states.write_queue.add_work(request->rpc_code(), request, this);
    if (mu) {
        init_prepare(mu, false);
    }
}

void replica::init_prepare(mutation_ptr &mu, bool reconciliation, bool pop_all_committed_mutations)
{
    dassert(partition_status::PS_PRIMARY == status(),
            "invalid partition_status, status = %s",
            enum_to_string(status()));

    mu->_tracer->set_description("primary");
    ADD_POINT(mu->_tracer);

    error_code err = ERR_OK;
    uint8_t count = 0;
    const auto request_count = mu->client_requests.size();
    mu->data.header.last_committed_decree = last_committed_decree();

    dsn_log_level_t level = LOG_LEVEL_INFORMATION;
    if (mu->data.header.decree == invalid_decree) {
        mu->set_id(get_ballot(), _prepare_list->max_decree() + 1);
        // print a debug log if necessary
        if (_options->prepare_decree_gap_for_debug_logging > 0 &&
            mu->get_decree() % _options->prepare_decree_gap_for_debug_logging == 0)
            level = LOG_LEVEL_DEBUG;
        mu->set_timestamp(_uniq_timestamp_us.next());
    } else {
        mu->set_id(get_ballot(), mu->data.header.decree);
    }

    mu->_tracer->set_name(fmt::format("mutation[{}]", mu->name()));
    dlog(level,
         "%s: mutation %s init_prepare, mutation_tid=%" PRIu64,
         name(),
         mu->name(),
         mu->tid());

    // child should prepare mutation synchronously
    mu->set_is_sync_to_child(_primary_states.sync_send_write_request);

    // check bounded staleness
    if (mu->data.header.decree > last_committed_decree() + _options->staleness_for_commit) {
        err = ERR_CAPACITY_EXCEEDED;
        goto ErrOut;
    }

    // stop prepare bulk load ingestion if there are secondaries unalive
    for (auto i = 0; i < request_count; ++i) {
        const mutation_update &update = mu->data.updates[i];
        if (update.code != dsn::apps::RPC_RRDB_RRDB_BULK_LOAD) {
            break;
        }
        ddebug_replica("try to prepare bulk load mutation({})", mu->name());
        if (static_cast<int>(_primary_states.membership.secondaries.size()) + 1 <
            _primary_states.membership.max_replica_count) {
            err = ERR_NOT_ENOUGH_MEMBER;
            break;
        }
    }
    if (err != ERR_OK) {
        goto ErrOut;
    }

    // stop prepare if there are too few replicas unless it's a reconciliation
    // for reconciliation, we should ensure every prepared mutation to be committed
    // please refer to PacificA paper
    if (static_cast<int>(_primary_states.membership.secondaries.size()) + 1 <
            _options->app_mutation_2pc_min_replica_count(_app_info.max_replica_count) &&
        !reconciliation) {
        err = ERR_NOT_ENOUGH_MEMBER;
        goto ErrOut;
    }

    dassert(mu->data.header.decree > last_committed_decree(),
            "%" PRId64 " VS %" PRId64 "",
            mu->data.header.decree,
            last_committed_decree());

    // local prepare
    err = _prepare_list->prepare(mu, partition_status::PS_PRIMARY, pop_all_committed_mutations);
    if (err != ERR_OK) {
        goto ErrOut;
    }

    // remote prepare
    mu->set_prepare_ts();
    mu->set_left_secondary_ack_count((unsigned int)_primary_states.membership.secondaries.size());
    for (auto it = _primary_states.membership.secondaries.begin();
         it != _primary_states.membership.secondaries.end();
         ++it) {
        send_prepare_message(*it,
                             partition_status::PS_SECONDARY,
                             mu,
                             _options->prepare_timeout_ms_for_secondaries,
                             pop_all_committed_mutations);
    }

    count = 0;
    for (auto it = _primary_states.learners.begin(); it != _primary_states.learners.end(); ++it) {
        if (it->second.prepare_start_decree != invalid_decree &&
            mu->data.header.decree >= it->second.prepare_start_decree) {
            send_prepare_message(it->first,
                                 partition_status::PS_POTENTIAL_SECONDARY,
                                 mu,
                                 _options->prepare_timeout_ms_for_potential_secondaries,
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
        dassert(mu->data.header.log_offset == invalid_offset,
                "invalid log offset, offset = %" PRId64,
                mu->data.header.log_offset);
        dassert(mu->log_task() == nullptr, "");
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
        dassert(nullptr != mu->log_task(), "");
        if (_options->log_shared_pending_size_throttling_threshold_kb > 0 &&
            _options->log_shared_pending_size_throttling_delay_ms > 0 &&
            pending_size >= _options->log_shared_pending_size_throttling_threshold_kb * 1024) {
            int delay_ms = _options->log_shared_pending_size_throttling_delay_ms;
            for (dsn::message_ex *r : mu->client_requests) {
                if (r && r->io_session->delay_recv(delay_ms)) {
                    dwarn("too large pending shared log (%" PRId64 "), "
                          "delay traffic from %s for %d milliseconds",
                          pending_size,
                          r->header->from_address.to_string(),
                          delay_ms);
                }
            }
        }
    }

    _primary_states.last_prepare_ts_ms = mu->prepare_ts_ms();
    return;

ErrOut:
    for (auto &r : mu->client_requests) {
        response_client_write(r, err);
    }
    return;
}

void replica::send_prepare_message(::dsn::rpc_address addr,
                                   partition_status::type status,
                                   const mutation_ptr &mu,
                                   int timeout_milliseconds,
                                   bool pop_all_committed_mutations,
                                   int64_t learn_signature)
{
    mu->_tracer->add_sub_tracer(addr.to_string());
    ADD_POINT(mu->_tracer->sub_tracer(addr.to_string()));

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

    mu->remote_tasks()[addr] =
        rpc::call(addr,
                  msg,
                  &_tracker,
                  [=](error_code err, dsn::message_ex *request, dsn::message_ex *reply) {
                      on_prepare_reply(std::make_pair(mu, rconfig.status), err, request, reply);
                  },
                  get_gpid().thread_hash());

    dinfo("%s: mutation %s send_prepare_message to %s as %s",
          name(),
          mu->name(),
          addr.to_string(),
          enum_to_string(rconfig.status));
}

void replica::do_possible_commit_on_primary(mutation_ptr &mu)
{
    dassert(_config.ballot == mu->data.header.ballot,
            "invalid mutation ballot, %" PRId64 " VS %" PRId64 "",
            _config.ballot,
            mu->data.header.ballot);
    dassert(partition_status::PS_PRIMARY == status(),
            "invalid partition_status, status = %s",
            enum_to_string(status()));

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

    dinfo("%s: mutation %s on_prepare", name(), mu->name());
    mu->_tracer->set_name(fmt::format("mutation[{}]", mu->name()));
    mu->_tracer->set_description("secondary");
    ADD_POINT(mu->_tracer);

    dassert(mu->data.header.pid == rconfig.pid,
            "(%d.%d) VS (%d.%d)",
            mu->data.header.pid.get_app_id(),
            mu->data.header.pid.get_partition_index(),
            rconfig.pid.get_app_id(),
            rconfig.pid.get_partition_index());
    dassert(mu->data.header.ballot == rconfig.ballot,
            "invalid mutation ballot, %" PRId64 " VS %" PRId64 "",
            mu->data.header.ballot,
            rconfig.ballot);

    if (mu->data.header.ballot < get_ballot()) {
        derror("%s: mutation %s on_prepare skipped due to old view", name(), mu->name());
        // no need response because the rpc should have been cancelled on primary in this case
        return;
    }

    // update configuration when necessary
    else if (rconfig.ballot > get_ballot()) {
        if (!update_local_configuration(rconfig)) {
            derror("%s: mutation %s on_prepare failed as update local configuration failed, state "
                   "= %s",
                   name(),
                   mu->name(),
                   enum_to_string(status()));
            ack_prepare_message(ERR_INVALID_STATE, mu);
            return;
        }
    }

    if (partition_status::PS_INACTIVE == status() || partition_status::PS_ERROR == status()) {
        derror("%s: mutation %s on_prepare failed as invalid replica state, state = %s",
               name(),
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
            derror("%s: mutation %s on_prepare failed as unmatched learning signature, state = %s"
                   ", old_signature[%016" PRIx64 "] vs new_signature[%016" PRIx64 "]",
                   name(),
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
            derror("%s: mutation %s on_prepare skipped as invalid learning status, state = %s, "
                   "learning_status = %s, ack %s",
                   name(),
                   mu->name(),
                   enum_to_string(status()),
                   enum_to_string(learning_status),
                   ack_code.to_string());
            ack_prepare_message(ack_code, mu);
            return;
        }
    }

    dassert(rconfig.status == status(),
            "invalid status, %s VS %s",
            enum_to_string(rconfig.status),
            enum_to_string(status()));
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
    dassert(err == ERR_OK, "prepare mutation failed, err = %s", err.to_string());

    if (partition_status::PS_POTENTIAL_SECONDARY == status() ||
        partition_status::PS_SECONDARY == status()) {
        dassert(mu->data.header.decree <=
                    last_committed_decree() + _options->max_mutation_count_in_prepare_list,
                "%" PRId64 " VS %" PRId64 "(%" PRId64 " + %d)",
                mu->data.header.decree,
                last_committed_decree() + _options->max_mutation_count_in_prepare_list,
                last_committed_decree(),
                _options->max_mutation_count_in_prepare_list);
    } else {
        derror("%s: mutation %s on_prepare failed as invalid replica state, state = %s",
               name(),
               mu->name(),
               enum_to_string(status()));
        ack_prepare_message(ERR_INVALID_STATE, mu);
        return;
    }

    if (_split_mgr->is_splitting()) {
        _split_mgr->copy_mutation(mu);
    }

    dassert(mu->log_task() == nullptr, "");
    mu->log_task() = _private_log->append(mu,
                                          LPC_WRITE_REPLICATION_LOG,
                                          &_tracker,
                                          std::bind(&replica::on_append_log_completed,
                                                    this,
                                                    mu,
                                                    std::placeholders::_1,
                                                    std::placeholders::_2),
                                          get_gpid().thread_hash());
    dassert(nullptr != mu->log_task(), "");
}

void replica::on_append_log_completed(mutation_ptr &mu, error_code err, size_t size)
{
    _checker.only_one_thread_access();

    dinfo("%s: append shared log completed for mutation %s, size = %u, err = %s",
          name(),
          mu->name(),
          size,
          err.to_string());

    ADD_POINT(mu->_tracer);

    if (err == ERR_OK) {
        mu->set_logged();
    } else {
        derror("%s: append shared log failed for mutation %s, err = %s",
               name(),
               mu->name(),
               err.to_string());
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
            dassert(false, "invalid partition_status, status = %s", enum_to_string(status()));
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

    dassert(mu->data.header.ballot == get_ballot(),
            "%s: invalid mutation ballot, %" PRId64 " VS %" PRId64 "",
            mu->name(),
            mu->data.header.ballot,
            get_ballot());

    ::dsn::rpc_address node = request->to_address;
    partition_status::type st = _primary_states.get_node_status(node);

    // handle reply
    prepare_ack resp;

    // handle error
    if (err != ERR_OK) {
        resp.err = err;
    } else {
        ::dsn::unmarshall(reply, resp);
    }

    auto send_prepare_tracer = mu->_tracer->sub_tracer(request->to_address.to_string());
    APPEND_EXTERN_POINT(send_prepare_tracer, resp.receive_timestamp, "remote_receive");
    APPEND_EXTERN_POINT(send_prepare_tracer, resp.response_timestamp, "remote_reply");
    ADD_CUSTOM_POINT(send_prepare_tracer, resp.err.to_string());

    if (resp.err == ERR_OK) {
        dinfo("%s: mutation %s on_prepare_reply from %s, appro_data_bytes = %d, "
              "target_status = %s, err = %s",
              name(),
              mu->name(),
              node.to_string(),
              mu->appro_data_bytes(),
              enum_to_string(target_status),
              resp.err.to_string());
    } else {
        derror("%s: mutation %s on_prepare_reply from %s, appro_data_bytes = %d, "
               "target_status = %s, err = %s",
               name(),
               mu->name(),
               node.to_string(),
               mu->appro_data_bytes(),
               enum_to_string(target_status),
               resp.err.to_string());
    }

    if (resp.err == ERR_OK) {
        dassert(resp.ballot == get_ballot(),
                "invalid response ballot, %" PRId64 " VS %" PRId64 "",
                resp.ballot,
                get_ballot());
        dassert(resp.decree == mu->data.header.decree,
                "invalid response decree, %" PRId64 " VS %" PRId64 "",
                resp.decree,
                mu->data.header.decree);

        switch (target_status) {
        case partition_status::PS_SECONDARY:
            dassert(_primary_states.check_exist(node, partition_status::PS_SECONDARY),
                    "invalid secondary node address, address = %s",
                    node.to_string());
            dassert(mu->left_secondary_ack_count() > 0, "%u", mu->left_secondary_ack_count());
            if (0 == mu->decrease_left_secondary_ack_count()) {
                do_possible_commit_on_primary(mu);
            }
            break;
        case partition_status::PS_POTENTIAL_SECONDARY:
            dassert(mu->left_potential_secondary_ack_count() > 0,
                    "%u",
                    mu->left_potential_secondary_ack_count());
            if (0 == mu->decrease_left_potential_secondary_ack_count()) {
                do_possible_commit_on_primary(mu);
            }
            break;
        default:
            dwarn("%s: mutation %s prepare ack skipped coz the node is now inactive",
                  name(),
                  mu->name());
            break;
        }
    }

    // failure handling
    else {
        // retry for INACTIVE or TRY_AGAIN if there is still time.
        if (resp.err == ERR_INACTIVE_STATE || resp.err == ERR_TRY_AGAIN) {
            int prepare_timeout_ms = (target_status == partition_status::PS_SECONDARY
                                          ? _options->prepare_timeout_ms_for_secondaries
                                          : _options->prepare_timeout_ms_for_potential_secondaries);
            int delay_time_ms = 5; // delay some time before retry to avoid sending too frequently
            if (mu->is_prepare_close_to_timeout(delay_time_ms + 2, prepare_timeout_ms)) {
                derror("%s: mutation %s do not retry prepare to %s for no enought time left, "
                       "prepare_ts_ms = %" PRIu64 ", prepare_timeout_ms = %d, now_ms = %" PRIu64,
                       name(),
                       mu->name(),
                       node.to_string(),
                       mu->prepare_ts_ms(),
                       prepare_timeout_ms,
                       dsn_now_ms());
            } else {
                ddebug("%s: mutation %s retry prepare to %s after %d ms",
                       name(),
                       mu->name(),
                       node.to_string(),
                       delay_time_ms);
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

        _stub->_counter_replicas_recent_prepare_fail_count->increment();

        // make sure this is before any later commit ops
        // because now commit ops may lead to new prepare ops
        // due to replication throttling
        handle_remote_failure(st, node, resp.err, "prepare");

        // note targetStatus and (curent) status may diff
        if (target_status == partition_status::PS_POTENTIAL_SECONDARY) {
            dassert(mu->left_potential_secondary_ack_count() > 0,
                    "%u",
                    mu->left_potential_secondary_ack_count());
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
    dassert(!prepare_requests.empty(), "mutation = %s", mu->name());

    if (err == ERR_OK) {
        if (mu->is_child_acked()) {
            dinfo_replica("mutation {} ack_prepare_message, err = {}", mu->name(), err);
            for (auto &request : prepare_requests) {
                reply(request, resp);
            }
        }
        return;
    }
    // only happened when prepare failed during partition split child copy mutation synchronously
    if (mu->is_error_acked()) {
        dwarn_replica("mutation {} has been ack_prepare_message, err = {}", mu->name(), err);
        return;
    }

    dwarn_replica("mutation {} ack_prepare_message, err = {}", mu->name(), err);
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
