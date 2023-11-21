// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "utils/metrics.h"

#include <absl/strings/string_view.h>
#include <boost/asio/basic_deadline_timer.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/system/error_code.hpp>
#include <fmt/core.h>
#include <new>

#include "http/http_method.h"
#include "runtime/api_layer1.h"
#include "utils/flags.h"
#include "utils/rand.h"
#include "utils/shared_io_service.h"
#include "utils/string_conv.h"
#include "utils/strings.h"

namespace dsn {

DSN_DEFINE_uint64(metrics,
                  entity_retirement_delay_ms,
                  10 * 60 * 1000,
                  "The retention internal (milliseconds) for an entity after it becomes stale.");

metric_entity::metric_entity(const metric_entity_prototype *prototype,
                             const std::string &id,
                             const attr_map &attrs)
    : _prototype(prototype), _id(id), _attrs(attrs), _retire_time_ms(0)
{
}

metric_entity::~metric_entity()
{
    // We have to wait for all of close operations to be finished. Waiting for close operations to
    // be finished in the destructor of each metirc may lead to memory leak detected in ASAN test
    // for dsn_utils_test, since the percentile is also referenced by shared_io_service which is
    // still alive without being destructed after ASAN test for dsn_utils_test is finished.
    close(close_option::kWait);
}

void metric_entity::close(close_option option)
{
    utils::auto_write_lock l(_lock);

    // To close all metrics owned by an entity, it's more efficient to firstly issue an asynchronous
    // close request to each metric; then, just wait for all of the close operations to be finished.
    // It's inefficient to wait for each metric to be closed one by one. Therefore, the metric is
    // not closed in its destructor.
    for (auto &m : _metrics) {
        if (m.second->prototype()->type() == metric_type::kPercentile) {
            auto p = down_cast<closeable_metric *>(m.second.get());
            p->close();
        }
    }

    if (option == close_option::kNoWait) {
        return;
    }

    // Wait for all of the close operations to be finished.
    for (auto &m : _metrics) {
        if (m.second->prototype()->type() == metric_type::kPercentile) {
            auto p = down_cast<closeable_metric *>(m.second.get());
            p->wait();
        }
    }
}

metric_entity::attr_map metric_entity::attributes() const
{
    utils::auto_read_lock l(_lock);
    return _attrs;
}

metric_entity::metric_map metric_entity::metrics() const
{
    utils::auto_read_lock l(_lock);
    return _metrics;
}

void metric_entity::set_attributes(const attr_map &attrs)
{
    utils::auto_write_lock l(_lock);
    _attrs = attrs;
}

void metric_entity::encode_type(metric_json_writer &writer) const
{
    writer.Key(kMetricEntityTypeField.c_str());
    json::json_encode(writer, _prototype->name());
}

void metric_entity::encode_id(metric_json_writer &writer) const
{
    writer.Key(kMetricEntityIdField.c_str());
    json::json_encode(writer, _id);
}

namespace {

void encode_attrs(dsn::metric_json_writer &writer, const dsn::metric_entity::attr_map &attrs)
{
    // Empty attributes are allowed and will just be encoded as {}.

    writer.Key(dsn::kMetricEntityAttrsField.c_str());

    writer.StartObject();
    for (const auto &attr : attrs) {
        writer.Key(attr.first.c_str());
        dsn::json::json_encode(writer, attr.second);
    }
    writer.EndObject();
}

void encode_metrics(dsn::metric_json_writer &writer,
                    const dsn::metric_entity::metric_map &metrics,
                    const dsn::metric_filters &filters)
{
    // We shouldn't reach here if no metric is chosen, thus just mark an assertion.
    CHECK(!metrics.empty(),
          "this entity should not be encoded into the response since no metric is chosen");

    writer.Key(dsn::kMetricEntityMetricsField.c_str());

    writer.StartArray();
    for (const auto &m : metrics) {
        m.second->take_snapshot(writer, filters);
    }
    writer.EndArray();
}

} // anonymous namespace

void metric_entity::take_snapshot(metric_json_writer &writer, const metric_filters &filters) const
{
    if (!filters.match_entity_type(_prototype->name())) {
        return;
    }

    if (!filters.match_entity_id(_id)) {
        return;
    }

    attr_map my_attrs;
    metric_map target_metrics;

    {
        utils::auto_read_lock l(_lock);

        if (!filters.match_entity_attrs(_attrs)) {
            return;
        }

        filters.extract_entity_metrics(_metrics, target_metrics);
        if (target_metrics.empty()) {
            // None of metrics is chosen, there is no need to take snapshot for
            // this entity.
            return;
        }

        my_attrs = _attrs;
    }

    // At least one metric of this entity has been chosen, thus take snapshot and encode
    // this entity as json format.
    writer.StartObject();
    encode_type(writer);
    encode_id(writer);
    encode_attrs(writer, my_attrs);
    encode_metrics(writer, target_metrics, filters);
    writer.EndObject();
}

bool metric_entity::is_stale() const
{
    // Since this entity itself is still being accessed, its reference count should be 1
    // at least.
    CHECK_GE(get_count(), 1);

    // This entity is considered stale once there is only one reference for it kept in the
    // registry.
    return get_count() == 1;
}

void metric_filters::extract_entity_metrics(const metric_entity::metric_map &candidates,
                                            metric_entity::metric_map &target_metrics) const
{
    if (entity_metrics.empty()) {
        target_metrics = candidates;
        return;
    }

    target_metrics.clear();
    for (const auto &candidate : candidates) {
        if (match(candidate.first->name().data(), entity_metrics)) {
            target_metrics.emplace(candidate.first, candidate.second);
        }
    }
}

metric_entity_ptr metric_entity_prototype::instantiate(const std::string &id,
                                                       const metric_entity::attr_map &attrs) const
{
    return metric_registry::instance().find_or_create_entity(this, id, attrs);
}

metric_entity_ptr metric_entity_prototype::instantiate(const std::string &id) const
{
    return instantiate(id, {});
}

metric_entity_prototype::metric_entity_prototype(const char *name) : _name(name) {}

metric_entity_prototype::~metric_entity_prototype() {}

metrics_http_service::metrics_http_service(metric_registry *registry) : _registry(registry)
{
    register_handler("metrics",
                     std::bind(&metrics_http_service::get_metrics_handler,
                               this,
                               std::placeholders::_1,
                               std::placeholders::_2),
                     "ip:port/metrics");
}

namespace {

template <typename Container>
void parse_as(const std::string &field_value, Container &container)
{
    utils::split_args(field_value.c_str(), container, ',');
}

inline void encode_error(dsn::metric_json_writer &writer, const char *error_message)
{
    writer.StartObject();
    writer.Key("error_message");
    dsn::json::json_encode(writer, error_message);
    writer.EndObject();
}

inline std::string encode_error_as_json(const char *error_message)
{
    return encode_as_json(
        [error_message](metric_json_writer &writer) { encode_error(writer, error_message); });
}

dsn::metric_filters::metric_fields_type get_brief_metric_fields()
{
    dsn::metric_filters::metric_fields_type fields = {kMetricNameField, kMetricSingleValueField};
    for (const auto &kth : kAllKthPercentiles) {
        fields.insert(kth.name);
    }
    return fields;
}

const dsn::metric_filters::metric_fields_type kBriefMetricFields = get_brief_metric_fields();

} // anonymous namespace

void metrics_http_service::get_metrics_handler(const http_request &req, http_response &resp)
{
    if (req.method != http_method::GET) {
        resp.body = encode_error_as_json("please use 'GET' method while querying for metrics");
        resp.status_code = http_status_code::bad_request;
        return;
    }

    metric_filters filters;
    bool with_metric_fields = false;
    bool detail = false;
    for (const auto &field : req.query_args) {
        if (field.first == "with_metric_fields") {
            parse_as(field.second, filters.with_metric_fields);
            with_metric_fields = true;
        } else if (field.first == "types") {
            parse_as(field.second, filters.entity_types);
        } else if (field.first == "ids") {
            parse_as(field.second, filters.entity_ids);
        } else if (field.first == "attributes") {
            parse_as(field.second, filters.entity_attrs);
            if ((filters.entity_attrs.size() & 1) != 0) {
                resp.body =
                    encode_error_as_json("the number of arguments for attributes should be even, "
                                         "since each attribute name always pairs with a value");
                resp.status_code = http_status_code::bad_request;
                return;
            }
        } else if (field.first == "metrics") {
            parse_as(field.second, filters.entity_metrics);
        } else if (field.first == "detail") {
            if (!buf2bool(field.second, detail)) {
                resp.body = encode_error_as_json("the value of detail should be a boolean value, "
                                                 "i.e. true or false");
                resp.status_code = http_status_code::bad_request;
                return;
            }
        } else {
            auto error_message = fmt::format("unknown field {}={}", field.first, field.second);
            resp.body = encode_error_as_json(error_message.c_str());
            resp.status_code = http_status_code::bad_request;
            return;
        }
    }

    // `with_metric_fields` takes precedence over `detail`: once `with_metric_fields` is
    // specified, it will be considered firstly.
    if (!with_metric_fields && !detail) {
        filters.with_metric_fields = kBriefMetricFields;
    }

    resp.body = take_snapshot_as_json(_registry, filters);
    resp.status_code = http_status_code::ok;
}

metric_registry::metric_registry() : _http_service(this)
{
    // We should ensure that metric_registry is destructed before shared_io_service is destructed.
    // Once shared_io_service is destructed before metric_registry is destructed,
    // boost::asio::io_service needed by metrics in metric_registry such as metric_timer will
    // be released firstly, then will lead to heap-use-after-free error since percentiles in
    // metric_registry are still running but the resources they needed have been released.
    tools::shared_io_service::instance();

    start_timer();
}

metric_registry::~metric_registry()
{
    utils::auto_write_lock l(_lock);

    // Once the registery is chosen to be destructed, all of the entities and metrics owned by it
    // will no longer be needed.
    //
    // The reason why each entity is closed in the registery rather than in the destructor of each
    // entity is that close(kNoWait) for the entity will return immediately without waiting for any
    // close operation to be finished.
    //
    // Thus, to close all entities owned by a registery, it's more efficient to firstly issue a
    // close request for all entities; then, just wait for all of the close operations to be
    // finished in the destructor of each entity. It's inefficient to wait for each entity to be
    // closed one by one.
    for (auto &entity : _entities) {
        entity.second->close(metric_entity::close_option::kNoWait);
    }

    stop_timer();
}

void metric_registry::on_close() {}

void metric_registry::start_timer()
{
    if (_timer) {
        return;
    }

    // Once an entity is considered stale, it will be retired after the retention interval,
    // namely FLAGS_entity_retirement_delay_ms milliseconds. Therefore, if the interval of
    // the timer is also set to FLAGS_entity_retirement_delay_ms, in the next round, it's
    // just about time to retire this entity.
    _timer.reset(new metric_timer(FLAGS_entity_retirement_delay_ms,
                                  std::bind(&metric_registry::process_stale_entities, this),
                                  std::bind(&metric_registry::on_close, this)));
}

void metric_registry::stop_timer()
{
    if (!_timer) {
        return;
    }

    // Close the timer synchronously.
    _timer->close();
    _timer->wait();

    // Reset the timer to mark that it has been stopped, now it could be started.
    _timer.reset();
}

metric_registry::entity_map metric_registry::entities() const
{
    utils::auto_read_lock l(_lock);
    return _entities;
}

void metric_registry::take_snapshot(metric_json_writer &writer, const metric_filters &filters) const
{
    utils::auto_read_lock l(_lock);

    writer.StartArray();
    for (const auto &entity : _entities) {
        entity.second->take_snapshot(writer, filters);
    }
    writer.EndArray();
}

metric_entity_ptr metric_registry::find_or_create_entity(const metric_entity_prototype *prototype,
                                                         const std::string &id,
                                                         const metric_entity::attr_map &attrs)
{
    utils::auto_write_lock l(_lock);

    entity_map::const_iterator iter = _entities.find(id);

    metric_entity_ptr entity;
    if (iter == _entities.end()) {
        entity = new metric_entity(prototype, id, attrs);
        _entities[id] = entity;
    } else {
        CHECK_STREQ_MSG(
            prototype->name(),
            iter->second->prototype()->name(),
            "new prototype '{}' is inconsistent with old prototype '{}' for entity '{}'",
            prototype->name(),
            iter->second->prototype()->name(),
            id);

        iter->second->set_attributes(attrs);
        entity = iter->second;
    }

    return entity;
}

metric_registry::collected_entities_info metric_registry::collect_stale_entities() const
{
    collected_entities_info collected_info;

    auto now = dsn_now_ms();

    utils::auto_read_lock l(_lock);

    for (const auto &entity : _entities) {
        if (!entity.second->is_stale()) {
            if (entity.second->_retire_time_ms > 0) {
                // This entity had been scheduled to be retired. However, it was reemployed
                // after that. It has been in use since then, therefore its scheduled time
                // for retirement should be reset to 0.
                collected_info.collected_entities.insert(entity.first);
            }
            continue;
        }

        if (entity.second->_retire_time_ms > now) {
            // This entity has been scheduled to be retired, however it is still within
            // the retention interval. Thus do not collect it.
            ++collected_info.num_scheduled_entities;
            continue;
        }

        collected_info.collected_entities.insert(entity.first);
    }

    collected_info.num_all_entities = _entities.size();
    return collected_info;
}

metric_registry::retired_entities_stat
metric_registry::retire_stale_entities(const collected_entity_list &collected_entities)
{
    if (collected_entities.empty()) {
        // Do not lock for empty list.
        return retired_entities_stat();
    }

    retired_entities_stat retired_stat;

    auto now = dsn_now_ms();

    utils::auto_write_lock l(_lock);

    for (const auto &collected_entity : collected_entities) {
        auto iter = _entities.find(collected_entity);
        if (dsn_unlikely(iter == _entities.end())) {
            // The entity has been removed from the registry for some unusual reason.
            continue;
        }

        if (!iter->second->is_stale()) {
            if (iter->second->_retire_time_ms > 0) {
                // For those entities which are reemployed, their scheduled time for retirement
                // should be reset to 0 though previously they could have been scheduled to be
                // retired.
                iter->second->_retire_time_ms = 0;
                ++retired_stat.num_reemployed_entities;
            }
            continue;
        }

        if (dsn_unlikely(iter->second->_retire_time_ms > now)) {
            // Since in collect_stale_entities() we've filtered the metrics which have been
            // outside the retention interval, this is unlikely to happen. However, we still
            // check here.
            continue;
        }

        if (iter->second->_retire_time_ms == 0) {
            // The entity should be marked with a scheduled time for retirement, since it has
            // already been considered stale.
            iter->second->_retire_time_ms = now + FLAGS_entity_retirement_delay_ms;
            ++retired_stat.num_recently_scheduled_entities;
            continue;
        }

        // Once the entity is outside the retention interval, retire it from the registry.
        _entities.erase(iter);
        ++retired_stat.num_retired_entities;
    }

    return retired_stat;
}

void metric_registry::process_stale_entities()
{
    LOG_INFO("begin to process stale metric entities");

    const auto &collected_info = collect_stale_entities();
    const auto &retired_stat = retire_stale_entities(collected_info.collected_entities);

    LOG_INFO("stat for metric entities: total={}, collected={}, retired={}, scheduled={}, "
             "recently_scheduled={}, reemployed={}",
             collected_info.num_all_entities,
             collected_info.collected_entities.size(),
             retired_stat.num_retired_entities,
             collected_info.num_scheduled_entities,
             retired_stat.num_recently_scheduled_entities,
             retired_stat.num_reemployed_entities);
}

metric_prototype::metric_prototype(const ctor_args &args) : _args(args) {}

metric_prototype::~metric_prototype() {}

metric::metric(const metric_prototype *prototype) : _prototype(prototype) {}

closeable_metric::closeable_metric(const metric_prototype *prototype) : metric(prototype) {}

uint64_t metric_timer::generate_initial_delay_ms(uint64_t interval_ms)
{
    CHECK_GT(interval_ms, 0);

    if (interval_ms < 1000) {
        return rand::next_u64() % interval_ms + 50;
    }

    uint64_t interval_seconds = interval_ms / 1000;
    return (rand::next_u64() % interval_seconds + 1) * 1000 + rand::next_u64() % 1000;
}

metric_timer::metric_timer(uint64_t interval_ms, on_exec_fn on_exec, on_close_fn on_close)
    : _initial_delay_ms(generate_initial_delay_ms(interval_ms)),
      _interval_ms(interval_ms),
      _on_exec(on_exec),
      _on_close(on_close),
      _state(state::kRunning),
      _completed(),
      _timer(new boost::asio::deadline_timer(tools::shared_io_service::instance().ios))
{
    _timer->expires_from_now(boost::posix_time::milliseconds(_initial_delay_ms));
    _timer->async_wait(std::bind(&metric_timer::on_timer, this, std::placeholders::_1));
}

void metric_timer::close()
{
    // If the timer has already expired when cancel() is called, then the handlers for asynchronous
    // wait operations will:
    // * have already been invoked; or
    // * have been queued for invocation in the near future.
    //
    // These handlers can no longer be cancelled, and therefore are passed an error code that
    // indicates the successful completion of the wait operation. Thus set the state of timer to
    // kClosing to tell on_timer() that the timer should be closed even if it is not called with
    // operation_canceled.
    auto expected_state = state::kRunning;
    if (_state.compare_exchange_strong(expected_state, state::kClosing)) {
        _timer->cancel();
    }
}

void metric_timer::wait() { _completed.wait(); }

void metric_timer::on_close()
{
    _on_close();
    _completed.notify();
}

void metric_timer::on_timer(const boost::system::error_code &ec)
{
// This macro is defined for the case that handlers for asynchronous wait operations are no
// longer cancelled. It just checks the internal state atomically (since close() can also be
// called simultaneously) for kClosing; once it's matched, it will stop the timer by not executing
// any future handler.
#define TRY_PROCESS_TIMER_CLOSING()                                                                \
    do {                                                                                           \
        auto expected_state = state::kClosing;                                                     \
        if (_state.compare_exchange_strong(expected_state, state::kClosed)) {                      \
            on_close();                                                                            \
            return;                                                                                \
        }                                                                                          \
    } while (0)

    if (dsn_unlikely(!!ec)) {
        CHECK_EQ_MSG(static_cast<int>(boost::system::errc::operation_canceled),
                     ec.value(),
                     "failed to exec on_timer with an error that cannot be handled: {}",
                     ec.message());

        // Cancel can only be launched by close().
        auto expected_state = state::kClosing;
        CHECK(_state.compare_exchange_strong(expected_state, state::kClosed),
              "wrong state for metric_timer: {}, while expecting closing state",
              static_cast<int>(expected_state));
        on_close();

        return;
    }

    TRY_PROCESS_TIMER_CLOSING();
    _on_exec();

    TRY_PROCESS_TIMER_CLOSING();
    _timer->expires_from_now(boost::posix_time::milliseconds(_interval_ms));
    _timer->async_wait(std::bind(&metric_timer::on_timer, this, std::placeholders::_1));
#undef TRY_PROCESS_TIMER_CLOSING
}

} // namespace dsn
