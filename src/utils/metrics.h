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

#pragma once

#include <algorithm>
#include <atomic>
#include <bitset>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/asio/deadline_timer.hpp>

#include "common/json_helper.h"
#include "http/http_server.h"
#include "utils/api_utilities.h"
#include "utils/alloc.h"
#include "utils/autoref_ptr.h"
#include "utils/casts.h"
#include "utils/enum_helper.h"
#include "utils/fmt_logging.h"
#include "utils/long_adder.h"
#include "utils/nth_element.h"
#include "utils/ports.h"
#include "utils/singleton.h"
#include "utils/string_view.h"
#include "utils/strings.h"
#include "utils/synchronize.h"

// A metric library (for details pls see https://github.com/apache/incubator-pegasus/issues/922)
// inspired by Kudu metrics (https://github.com/apache/kudu/blob/master/src/kudu/util/metrics.h).
//
//
// Example of defining and instantiating a metric entity
// -----------------------------------------------------
// Define an entity type at the top of your .cpp file (not within any namespace):
// METRIC_DEFINE_entity(my_entity);
//
// To use the entity type, declare it at the top of any .h/.cpp file (not within any namespace):
// METRIC_DECLARE_entity(my_entity);
//
// Instantiating the entity in whatever class represents it:
// entity_instance = METRIC_ENTITY_my_entity.instantiate(my_entity_id, ...);
//
//
// Example of defining and instantiating a metric
// -----------------------------------------------------
// Define an entity type at the top of your .cpp file (not within any namespace):
// METRIC_DEFINE_gauge_int64(my_entity,
//                           my_gauge_name,
//                           dsn::metric_unit::kMilliSeconds,
//                           "the description for my gauge");
//
// To use the metric prototype, declare it at the top of any .h/.cpp file (not within any
// namespace):
// METRIC_DECLARE_gauge_int64(my_gauge_name);
//
// Instantiating the metric in whatever class represents it with some initial arguments, if any:
// metric_instance = METRIC_my_gauge_name.instantiate(entity_instance, ...);

// Convenient macros are provided to define entity types and metric prototypes.
#define METRIC_DEFINE_entity(name) ::dsn::metric_entity_prototype METRIC_ENTITY_##name(#name)
#define METRIC_DEFINE_gauge_int64(entity_type, name, unit, desc, ...)                              \
    ::dsn::gauge_prototype<int64_t> METRIC_##name(                                                 \
        {#entity_type, dsn::metric_type::kGauge, #name, unit, desc, ##__VA_ARGS__})
#define METRIC_DEFINE_gauge_double(entity_type, name, unit, desc, ...)                             \
    ::dsn::gauge_prototype<double> METRIC_##name(                                                  \
        {#entity_type, dsn::metric_type::kGauge, #name, unit, desc, ##__VA_ARGS__})
// There are 2 kinds of counters:
// - `counter` is the general type of counter that is implemented by striped_long_adder, which can
//   achieve high performance while consuming less memory if it's not updated very frequently.
// - `concurrent_counter` uses concurrent_long_adder as the underlying implementation. It has
//   higher performance while consuming more memory if it's updated very frequently.
// See also include/dsn/utility/long_adder.h for details.
#define METRIC_DEFINE_counter(entity_type, name, unit, desc, ...)                                  \
    dsn::counter_prototype<dsn::striped_long_adder, false> METRIC_##name(                          \
        {#entity_type, dsn::metric_type::kCounter, #name, unit, desc, ##__VA_ARGS__})
#define METRIC_DEFINE_concurrent_counter(entity_type, name, unit, desc, ...)                       \
    dsn::counter_prototype<dsn::concurrent_long_adder, false> METRIC_##name(                       \
        {#entity_type, dsn::metric_type::kCounter, #name, unit, desc, ##__VA_ARGS__})
#define METRIC_DEFINE_volatile_counter(entity_type, name, unit, desc, ...)                         \
    dsn::counter_prototype<dsn::striped_long_adder, true> METRIC_##name(                           \
        {#entity_type, dsn::metric_type::kVolatileCounter, #name, unit, desc, ##__VA_ARGS__})
#define METRIC_DEFINE_concurrent_volatile_counter(entity_type, name, unit, desc, ...)              \
    dsn::counter_prototype<dsn::concurrent_long_adder, true> METRIC_##name(                        \
        {#entity_type, dsn::metric_type::kVolatileCounter, #name, unit, desc, ##__VA_ARGS__})

// The percentile supports both integral and floating types.
#define METRIC_DEFINE_percentile_int64(entity_type, name, unit, desc, ...)                         \
    dsn::percentile_prototype<int64_t> METRIC_##name(                                              \
        {#entity_type, dsn::metric_type::kPercentile, #name, unit, desc, ##__VA_ARGS__})
#define METRIC_DEFINE_percentile_double(entity_type, name, unit, desc, ...)                        \
    dsn::floating_percentile_prototype<double> METRIC_##name(                                      \
        {#entity_type, dsn::metric_type::kPercentile, #name, unit, desc, ##__VA_ARGS__})

// The following macros act as forward declarations for entity types and metric prototypes.
#define METRIC_DECLARE_entity(name) extern ::dsn::metric_entity_prototype METRIC_ENTITY_##name
#define METRIC_DECLARE_gauge_int64(name) extern ::dsn::gauge_prototype<int64_t> METRIC_##name
#define METRIC_DECLARE_gauge_double(name) extern ::dsn::gauge_prototype<double> METRIC_##name
#define METRIC_DECLARE_counter(name)                                                               \
    extern dsn::counter_prototype<dsn::striped_long_adder, false> METRIC_##name
#define METRIC_DECLARE_concurrent_counter(name)                                                    \
    extern dsn::counter_prototype<dsn::concurrent_long_adder, false> METRIC_##name
#define METRIC_DECLARE_volatile_counter(name)                                                      \
    extern dsn::counter_prototype<dsn::striped_long_adder, true> METRIC_##name
#define METRIC_DECLARE_concurrent_volatile_counter(name)                                           \
    extern dsn::counter_prototype<dsn::concurrent_long_adder, true> METRIC_##name
#define METRIC_DECLARE_percentile_int64(name)                                                      \
    extern dsn::percentile_prototype<int64_t> METRIC_##name
#define METRIC_DECLARE_percentile_double(name)                                                     \
    extern dsn::floating_percentile_prototype<double> METRIC_##name

namespace dsn {

class metric_prototype;
class metric;
using metric_ptr = ref_ptr<metric>;
struct metric_filters;
class metric_entity_prototype;

using metric_json_writer = dsn::json::PrettyJsonWriter;

const std::string kMetricEntityTypeField = "type";
const std::string kMetricEntityIdField = "id";
const std::string kMetricEntityAttrsField = "attributes";
const std::string kMetricEntityMetricsField = "metrics";

class metric_entity : public ref_counter
{
public:
    using attr_map = std::unordered_map<std::string, std::string>;
    using metric_map = std::unordered_map<const metric_prototype *, metric_ptr>;

    const metric_entity_prototype *prototype() const { return _prototype; }

    const std::string &id() const { return _id; }

    attr_map attributes() const;

    metric_map metrics() const;

    // `args` are the parameters that are used to construct the object of MetricType.
    template <typename MetricType, typename... Args>
    ref_ptr<MetricType> find_or_create(const metric_prototype *prototype, Args &&... args);

    void take_snapshot(metric_json_writer &writer, const metric_filters &filters) const;

private:
    friend class metric_registry;
    friend class ref_ptr<metric_entity>;
    friend class scoped_entity;

    metric_entity(const metric_entity_prototype *prototype,
                  const std::string &id,
                  const attr_map &attrs);

    ~metric_entity();

    // Close all "closeable" metrics owned by this entity.
    //
    // `option` is used to control how the close operations are performed:
    // * kWait:     close() will be blocked until all of the close operations are finished.
    // * kNoWait:   once the close requests are issued, close() will return immediately without
    //              waiting for any close operation to be finished.
    enum class close_option : int
    {
        kWait,
        kNoWait,
    };
    void close(close_option option);

    void set_attributes(const attr_map &attrs);

    void encode_type(metric_json_writer &writer) const;

    void encode_id(metric_json_writer &writer) const;

    // Decide if an entity is stale. An entity becomes stale if it is no longer used by any other
    // object.
    //
    // An entity could be bound to one or multiple objects. Once all of these objects are
    // destroyed, this entity will become stale, which means all of the metrics held by this
    // entity are also stale.
    //
    // For example, once a replica is removed, the replica entity (and all metrics it holds) will
    // become stale; then, this entity is scheduled to be retired after a configurable retention
    // interval; finally, this entity will be removed from the registry with all metrics it holds.
    bool is_stale() const;

    const metric_entity_prototype *const _prototype;
    const std::string _id;

    mutable utils::rw_lock_nr _lock;
    attr_map _attrs;
    metric_map _metrics;

    // The timestamp when this entity should be retired:
    // * default value is 0, which means this entity has not been scheduled to be retired;
    // * otherwise, non-zero value means this entity has been scheduled to be retired, and will
    // be retired at any time once current time has reached or exceeded this timestamp.
    uint64_t _retire_time_ms;

    DISALLOW_COPY_AND_ASSIGN(metric_entity);
};

using metric_entity_ptr = ref_ptr<metric_entity>;

// This struct includes a set of filters for both entities and metrics requested by client.
struct metric_filters
{
    using metric_fields_type = std::unordered_set<std::string>;
    using entity_types_type = std::vector<std::string>;
    using entity_ids_type = std::unordered_set<std::string>;
    using entity_attrs_type = std::vector<std::string>;
    using entity_metrics_type = std::vector<std::string>;

// NOTICE: empty `white_list` means every field is required by client.
#define RETURN_MATCHED_WITH_EMPTY_WHITE_LIST(white_list)                                           \
    do {                                                                                           \
        if (white_list.empty()) {                                                                  \
            return true;                                                                           \
        }                                                                                          \
    } while (0)

#define DEFINE_SIMPLE_MATCHER(name)                                                                \
    template <typename T>                                                                          \
    inline bool match_##name(const T &candidate) const                                             \
    {                                                                                              \
        return match(candidate, name##s);                                                          \
    }

    static inline bool match(const char *candidate, const std::vector<std::string> &white_list)
    {
        RETURN_MATCHED_WITH_EMPTY_WHITE_LIST(white_list);
        // Will use `bool operator==(const string &lhs, const char *rhs);` to compare each element
        // in `white_list` with `candidate`.
        return std::find(white_list.begin(), white_list.end(), candidate) != white_list.end();
    }

    static inline bool match(const std::string &candidate,
                             const std::unordered_set<std::string> &white_list)
    {
        RETURN_MATCHED_WITH_EMPTY_WHITE_LIST(white_list);
        return white_list.find(candidate) != white_list.end();
    }

    // According to the parameters requested by client, this function will filter metric
    // fields that will be put in the response.
    DEFINE_SIMPLE_MATCHER(with_metric_field)

    DEFINE_SIMPLE_MATCHER(entity_type)

    DEFINE_SIMPLE_MATCHER(entity_id)

    bool match_entity_attrs(const metric_entity::attr_map &candidates) const
    {
        RETURN_MATCHED_WITH_EMPTY_WHITE_LIST(entity_attrs);

        // The size of container must be divisible by 2, since attribute name always pairs
        // with value in it.
        CHECK_EQ(entity_attrs.size() & 1, 0);

        for (entity_attrs_type::size_type i = 0; i < entity_attrs.size(); i += 2) {
            const auto &iter = candidates.find(entity_attrs[i]);
            if (iter == candidates.end()) {
                continue;
            }
            if (iter->second == entity_attrs[i + 1]) {
                // It will be considered as matched once any attribute is matched
                // for both name and value.
                return true;
            }
        }
        return false;
    }

#undef DEFINE_SIMPLE_MATCHER
#undef RETURN_MATCHED_WITH_EMPTY_WHITE_LIST

    void extract_entity_metrics(const metric_entity::metric_map &candidates,
                                metric_entity::metric_map &target_metrics) const;

    // `with_metric_fields` includes all the metric fields that are wanted by client. If it
    // is empty, there will be no restriction: in other words, all fields owned by the metric
    // will be put in the response.
    metric_fields_type with_metric_fields;

    entity_types_type entity_types;

    entity_ids_type entity_ids;

    entity_attrs_type entity_attrs;

    entity_metrics_type entity_metrics;
};

inline std::string encode_as_json(std::function<void(metric_json_writer &)> encoder)
{
    std::ostringstream out;
    rapidjson::OStreamWrapper wrapper(out);
    metric_json_writer writer(wrapper);
    encoder(writer);
    return out.str();
}

template <typename T>
inline std::string take_snapshot_as_json(T *m, const metric_filters &filters)
{
    return encode_as_json(
        [m, &filters](metric_json_writer &writer) { m->take_snapshot(writer, filters); });
}

class metric_entity_prototype
{
public:
    explicit metric_entity_prototype(const char *name);
    ~metric_entity_prototype();

    const char *name() const { return _name; }

    // Create an entity with the given ID and attributes, if any.
    metric_entity_ptr instantiate(const std::string &id,
                                  const metric_entity::attr_map &attrs) const;
    metric_entity_ptr instantiate(const std::string &id) const;

private:
    const char *const _name;

    DISALLOW_COPY_AND_ASSIGN(metric_entity_prototype);
};

class metric_registry;
class metrics_http_service : public http_server_base
{
public:
    explicit metrics_http_service(metric_registry *registry);
    ~metrics_http_service() = default;

    // There is only one API now whose URI is "/metrics", thus just make
    // this URI as sub path while leaving the root path empty.
    std::string path() const override { return ""; }

private:
    friend void test_get_metrics_handler(const http_request &req, http_response &resp);

    void get_metrics_handler(const http_request &req, http_response &resp);

    metric_registry *_registry;

    DISALLOW_COPY_AND_ASSIGN(metrics_http_service);
};

// `metric_timer` is a timer class that runs metric-related computations periodically, such as
// calculating percentile, checking if there are stale entities. It accepts `on_exec` and
// `on_close` as the callbacks for execution and close.
//
// In case that all metrics (such as percentiles) are computed at the same time and lead to very
// high load, first calculation will be delayed at a random interval.
class metric_timer
{
public:
    enum class state : int
    {
        kRunning,
        kClosing,
        kClosed,
    };

    using on_exec_fn = std::function<void()>;
    using on_close_fn = std::function<void()>;

    metric_timer(uint64_t interval_ms, on_exec_fn on_exec, on_close_fn on_close);
    ~metric_timer() = default;

    void close();
    void wait();

    // Get the initial delay that is randomly generated by `generate_initial_delay_ms()`.
    uint64_t get_initial_delay_ms() const { return _initial_delay_ms; }

private:
    // Generate an initial delay randomly in case that all percentiles are computed at the
    // same time.
    static uint64_t generate_initial_delay_ms(uint64_t interval_ms);

    void on_close();

    void on_timer(const boost::system::error_code &ec);

    const uint64_t _initial_delay_ms;
    const uint64_t _interval_ms;
    const on_exec_fn _on_exec;
    const on_close_fn _on_close;
    std::atomic<state> _state;
    utils::notify_event _completed;
    std::unique_ptr<boost::asio::deadline_timer> _timer;

    DISALLOW_COPY_AND_ASSIGN(metric_timer);
};

class metric_registry : public utils::singleton<metric_registry>
{
public:
    using entity_map = std::unordered_map<std::string, metric_entity_ptr>;
    using collected_entity_list = std::unordered_set<std::string>;

    struct collected_entities_info
    {
        // The collected entities that will be processed by retire_stale_entities(). Following
        // kinds of entities will be collected:
        // * entities that should be retired immediately. The entities that are still within
        // the retention interval will not be collected.
        // * entities that were previously considered stale however have already been reemployed,
        // which means its retirement should be cancelled by retire_stale_entities().
        collected_entity_list collected_entities;

        // The number of all entities in the registry.
        size_t num_all_entities = 0;

        // The number of the entities that have been scheduled to be retired.
        size_t num_scheduled_entities = 0;

        collected_entities_info() = default;
    };

    struct retired_entities_stat
    {
        // The number of retired entities.
        size_t num_retired_entities = 0;

        // The number of entities that were recently considered stale and scheduled to be
        // retired.
        size_t num_recently_scheduled_entities = 0;

        // The number of the entities that had previously been scheduled to be retired and
        // were recently reemployed.
        size_t num_reemployed_entities = 0;

        retired_entities_stat() = default;
    };

    entity_map entities() const;

    void take_snapshot(metric_json_writer &writer, const metric_filters &filters) const;

private:
    friend class metric_entity_prototype;
    friend class utils::singleton<metric_registry>;

    friend void test_get_metrics_handler(const http_request &req, http_response &resp);
    friend class scoped_entity;
    friend class MetricsRetirementTest;

    metric_registry();
    ~metric_registry();

    void on_close();

    void start_timer();
    void stop_timer();

    metric_entity_ptr find_or_create_entity(const metric_entity_prototype *prototype,
                                            const std::string &id,
                                            const metric_entity::attr_map &attrs);

    // These functions are used to retire stale entities.
    //
    // Since retirement is infrequent, there tend to be no entity that should be retired.
    // Therefore, the whole retirement process is divided into two phases: "collect" and
    // "retire".
    //
    // At the first phase "collect", we just check if there are entities that:
    // * has become stale, but has not been scheduled to be retired, or
    // * should be retired immediately, or
    // * previously were scheduled to be retired, now has been reemployed.
    //
    // All operations in the first phase are read-only, needing just read lock which is more
    // lightweight. If some entities were found following above conditions, albeit infrequenly,
    // they would be collected to be processed at the next phase.
    //
    // Collected entities, if any, will be processed at the second phase "retire":
    // * stale entities will be schedule to be retired;
    // * the expired entities will be retired;
    // * reset the retirement timestamp to 0 for reemployed entities.
    collected_entities_info collect_stale_entities() const;
    retired_entities_stat retire_stale_entities(const collected_entity_list &collected_entities);
    void process_stale_entities();

    mutable utils::rw_lock_nr _lock;
    entity_map _entities;

    metrics_http_service _http_service;

    std::unique_ptr<metric_timer> _timer;

    DISALLOW_COPY_AND_ASSIGN(metric_registry);
};

// metric_type is needed while metrics are collected to monitoring systems. Generally
// each monitoring system has its own types of metrics: firstly we should know which
// type our metric belongs to; then we can know how to "translate" it to the specific
// monitoring system.
//
// On the other hand, it is also needed when some special operation should be done
// for a metric type. For example, percentile should be closed while it's no longer
// used.
enum class metric_type
{
    kGauge,
    kCounter,
    kVolatileCounter,
    kPercentile,
    kInvalidUnit,
};

ENUM_BEGIN(metric_type, metric_type::kInvalidUnit)
ENUM_REG_WITH_CUSTOM_NAME(metric_type::kGauge, gauge)
ENUM_REG_WITH_CUSTOM_NAME(metric_type::kCounter, counter)
ENUM_REG_WITH_CUSTOM_NAME(metric_type::kVolatileCounter, volatile_counter)
ENUM_REG_WITH_CUSTOM_NAME(metric_type::kPercentile, percentile)
ENUM_END(metric_type)

enum class metric_unit
{
    kNanoSeconds,
    kMicroSeconds,
    kMilliSeconds,
    kSeconds,
    kRequests,
    kInvalidUnit,
};

ENUM_BEGIN(metric_unit, metric_unit::kInvalidUnit)
ENUM_REG_WITH_CUSTOM_NAME(metric_unit::kNanoSeconds, nanoseconds)
ENUM_REG_WITH_CUSTOM_NAME(metric_unit::kMicroSeconds, microseconds)
ENUM_REG_WITH_CUSTOM_NAME(metric_unit::kMilliSeconds, milliseconds)
ENUM_REG_WITH_CUSTOM_NAME(metric_unit::kSeconds, seconds)
ENUM_REG_WITH_CUSTOM_NAME(metric_unit::kRequests, requests)
ENUM_END(metric_unit)

class metric_prototype
{
public:
    struct ctor_args
    {
        const string_view entity_type;
        const metric_type type;
        const string_view name;
        const metric_unit unit;
        const string_view desc;
    };

    string_view entity_type() const { return _args.entity_type; }

    metric_type type() const { return _args.type; }

    string_view name() const { return _args.name; }

    metric_unit unit() const { return _args.unit; }

    string_view description() const { return _args.desc; }

protected:
    explicit metric_prototype(const ctor_args &args);
    virtual ~metric_prototype();

private:
    const ctor_args _args;

    DISALLOW_COPY_AND_ASSIGN(metric_prototype);
};

// metric_prototype_with<MetricType> can help to implement the prototype of each type of metric
// to construct a metric object conveniently.
template <typename MetricType>
class metric_prototype_with : public metric_prototype
{
public:
    explicit metric_prototype_with(const ctor_args &args) : metric_prototype(args) {}
    virtual ~metric_prototype_with() = default;

    // Construct a metric object based on the instance of metric_entity.
    template <typename... Args>
    ref_ptr<MetricType> instantiate(const metric_entity_ptr &entity, Args &&... args) const
    {
        return entity->find_or_create<MetricType>(this, std::forward<Args>(args)...);
    }

private:
    DISALLOW_COPY_AND_ASSIGN(metric_prototype_with);
};

template <typename MetricType, typename... Args>
ref_ptr<MetricType> metric_entity::find_or_create(const metric_prototype *prototype,
                                                  Args &&... args)
{
    CHECK_STREQ_MSG(prototype->entity_type().data(),
                    _prototype->name(),
                    "the entity type '{}' of the metric '{}' is inconsistent with the prototype "
                    "'{}' of the attached entity '{}'",
                    prototype->entity_type().data(),
                    prototype->name().data(),
                    _prototype->name(),
                    _id);

    utils::auto_write_lock l(_lock);

    metric_map::const_iterator iter = _metrics.find(prototype);
    if (iter != _metrics.end()) {
        auto raw_ptr = down_cast<MetricType *>(iter->second.get());
        return raw_ptr;
    }

    ref_ptr<MetricType> ptr(new MetricType(prototype, std::forward<Args>(args)...));
    _metrics[prototype] = ptr;
    return ptr;
}

const std::string kMetricTypeField = "type";
const std::string kMetricNameField = "name";
const std::string kMetricUnitField = "unit";
const std::string kMetricDescField = "desc";
const std::string kMetricSingleValueField = "value";

// Base class for each type of metric.
// Every metric class should inherit from this class.
//
// User object should hold a ref_ptr of a metric, while the entity will hold another ref_ptr.
// The ref count of a metric may becomes 1, which means the metric is only held by the entity:
// After a period of configurable time, if the ref count is still 1, the metric will be dropped
// in that it's considered to be useless. During the period when the metric is retained, once
// the same one is instantiated again, it will not be removed; whether the metric is instantiated,
// however, its lastest value is visible.
class metric : public ref_counter
{
public:
    const metric_prototype *prototype() const { return _prototype; }

    // Take snapshot of each metric to collect current values as json format with fields chosen
    // by `filters`.
    virtual void take_snapshot(metric_json_writer &writer, const metric_filters &filters) = 0;

protected:
    explicit metric(const metric_prototype *prototype);
    virtual ~metric() = default;

    // Encode a metric field specified by `field_name` as json format. However, once the field
    // are not chosen by `filters`, this function will do nothing.
    template <typename T>
    static inline void encode(metric_json_writer &writer,
                              const std::string &field_name,
                              const T &value,
                              const metric_filters &filters)
    {
        if (!filters.match_with_metric_field(field_name)) {
            return;
        }

        writer.Key(field_name.c_str());
        json::json_encode(writer, value);
    }

    // Encode the metric type as json format, if it is chosen by `filters`.
    inline void encode_type(metric_json_writer &writer, const metric_filters &filters) const
    {
        encode(writer, kMetricTypeField, enum_to_string(prototype()->type()), filters);
    }

    // Encode the metric name as json format, if it is chosen by `filters`.
    inline void encode_name(metric_json_writer &writer, const metric_filters &filters) const
    {
        encode(writer, kMetricNameField, prototype()->name().data(), filters);
    }

    // Encode the metric unit as json format, if it is chosen by `filters`.
    inline void encode_unit(metric_json_writer &writer, const metric_filters &filters) const
    {
        encode(writer, kMetricUnitField, enum_to_string(prototype()->unit()), filters);
    }

    // Encode the metric description as json format, if it is chosen by `filters`.
    inline void encode_desc(metric_json_writer &writer, const metric_filters &filters) const
    {
        encode(writer, kMetricDescField, prototype()->description().data(), filters);
    }

    // Encode the metric prototype as json format, if some attributes in it are chosen by `filters`.
    inline void encode_prototype(metric_json_writer &writer, const metric_filters &filters) const
    {
        encode_type(writer, filters);
        encode_name(writer, filters);
        encode_unit(writer, filters);
        encode_desc(writer, filters);
    }

    // Encode the unique value of a metric as json format, if it is chosen by `filters`. Notice
    // that the metric should have only one value. like gauge and counter.
    template <typename T>
    static inline void
    encode_single_value(metric_json_writer &writer, const T &value, const metric_filters &filters)
    {
        encode(writer, kMetricSingleValueField, value, filters);
    }

    const metric_prototype *const _prototype;

private:
    friend class metric_entity;

    DISALLOW_COPY_AND_ASSIGN(metric);
};

// closeable_metric is a metric that implements close() method to execute some necessary close
// operations before the destructor is invoked. close() will return immediately without waiting
// for any close operation to be finished, while wait() is used to wait for all of the close
// operations to be finished.
//
// It's guaranteed that close() for each metric will be called before it is destructed. Generally
// both of close() and wait() are invoked by its manager, namely metric_entity.
class closeable_metric : public metric
{
public:
    virtual void close() = 0;
    virtual void wait() = 0;

protected:
    explicit closeable_metric(const metric_prototype *prototype);
    virtual ~closeable_metric() = default;

private:
    DISALLOW_COPY_AND_ASSIGN(closeable_metric);
};

// A gauge is a metric that represents a single numerical value that can arbitrarily go up and
// down. Usually there are 2 scenarios for a guage.
//
// Firstly, a gauge can be used as an instantaneous measurement of a discrete value. Typical
// usages in this scenario are current memory usage, the total capacity and available ratio of
// a disk, etc.
//
// Secondly, a gauge can be used as a counter that increases and decreases. In this scenario only
// integral types are supported, and its typical usages are the number of tasks in queues, current
// number of running manual compacts, etc.
template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
class gauge : public metric
{
public:
    using value_type = T;

    value_type value() const { return _value.load(std::memory_order_relaxed); }

    // The snapshot collected has following json format:
    // {
    //     "name": "<metric_name>",
    //     "value": ...
    // }
    // where "name" is the name of the gauge in string type, and "value" is just current value
    // of the gauge fetched by `value()`, in numeric types (i.e. integral or floating-point type,
    // determined by `value_type`).
    void take_snapshot(metric_json_writer &writer, const metric_filters &filters) override
    {
        writer.StartObject();

        encode_prototype(writer, filters);
        encode_single_value(writer, value(), filters);

        writer.EndObject();
    }

    void set(const value_type &val) { _value.store(val, std::memory_order_relaxed); }

    template <typename Int = value_type,
              typename = typename std::enable_if<std::is_integral<Int>::value>::type>
    void increment_by(Int x)
    {
        _value.fetch_add(x, std::memory_order_relaxed);
    }

    template <typename Int = value_type,
              typename = typename std::enable_if<std::is_integral<Int>::value>::type>
    void decrement_by(Int x)
    {
        increment_by(-x);
    }

    template <typename Int = value_type,
              typename = typename std::enable_if<std::is_integral<Int>::value>::type>
    void increment()
    {
        increment_by(1);
    }

    template <typename Int = value_type,
              typename = typename std::enable_if<std::is_integral<Int>::value>::type>
    void decrement()
    {
        increment_by(-1);
    }

protected:
    gauge(const metric_prototype *prototype, const value_type &initial_val)
        : metric(prototype), _value(initial_val)
    {
    }

    gauge(const metric_prototype *prototype) : gauge(prototype, value_type()) {}

    virtual ~gauge() = default;

private:
    friend class metric_entity;
    friend class ref_ptr<gauge<value_type>>;

    std::atomic<value_type> _value;

    DISALLOW_COPY_AND_ASSIGN(gauge);
};

template <typename T>
using gauge_ptr = ref_ptr<gauge<T>>;

template <typename T>
using gauge_prototype = metric_prototype_with<gauge<T>>;

// A counter in essence is a 64-bit integer that increases monotonically. It should be noted that
// the counter does not support to decrease. If decrease is needed, please consider to use the
// gauge instead.
//
// The counter can be typically used to measure the number of processed requests, which in the
// future can be help to compute the QPS. All counters start out at 0, and are non-negative
// since they are monotonic.
//
// `IsVolatile` is false by default. Once it's specified as true, the counter will be volatile.
// The value() function of a volatile counter will reset the counter atomically after its value
// is fetched. A volatile counter can also be called as a "recent" counter.
//
// Sometimes "recent" counters are needed, such as the number of recent failed beacons sent from
// replica server, the count of updating configurations of partitions recently, etc. The "recent"
// count can be considered to be the accumulated count since it has been fetched last by value().
//
// In most cases, a general (i.e. non-volatile) counter is enough, which means it can also work
// for "recent" counters. For example, in Prometheus, delta() can be used to compute "recent"
// count for a general counter. Therefore, declare a counter as volatile only when necessary.
template <typename Adder = striped_long_adder, bool IsVolatile = false>
class counter : public metric
{
public:
    // To decide which member function should be called by template parameter, the parameter
    // should be one of the class template parameters in case that the parameter is needed to
    // be written each time the member function is called.
    //
    // Using class template parameter to decide which member function should be called, another
    // function template parameter with the same meaning should be introduced, since the class
    // template parameter cannot be used as a function template parameter again and will lead
    // to compilation error.
    template <bool Volatile = IsVolatile,
              typename = typename std::enable_if<!Volatile && !IsVolatile>::type>
    int64_t value() const
    {
        return _adder.value();
    }

    template <bool Volatile = IsVolatile,
              typename = typename std::enable_if<Volatile && IsVolatile>::type>
    int64_t value()
    {
        return _adder.fetch_and_reset();
    }

    // The snapshot collected has following json format:
    // {
    //     "name": "<metric_name>",
    //     "value": ...
    // }
    // where "name" is the name of the counter in string type, and "value" is just current value
    // of the counter fetched by `value()`, in integral type (namely int64_t).
    void take_snapshot(metric_json_writer &writer, const metric_filters &filters) override
    {
        writer.StartObject();

        encode_prototype(writer, filters);
        encode_single_value(writer, value(), filters);

        writer.EndObject();
    }

    // NOTICE: x MUST be a non-negative integer.
    void increment_by(int64_t x)
    {
        CHECK_GE_MSG(x, 0, "delta({}) by increment for counter must be a non-negative integer", x);
        _adder.increment_by(x);
    }

    void increment() { _adder.increment(); }

    void reset() { _adder.reset(); }

protected:
    counter(const metric_prototype *prototype) : metric(prototype) {}

    virtual ~counter() = default;

private:
    friend class metric_entity;
    friend class ref_ptr<counter<Adder, IsVolatile>>;

    long_adder_wrapper<Adder> _adder;

    DISALLOW_COPY_AND_ASSIGN(counter);
};

template <typename Adder = striped_long_adder, bool IsVolatile = false>
using counter_ptr = ref_ptr<counter<Adder, IsVolatile>>;

template <bool IsVolatile = false>
using concurrent_counter_ptr = counter_ptr<concurrent_long_adder, IsVolatile>;

template <typename Adder = striped_long_adder, bool IsVolatile = false>
using counter_prototype = metric_prototype_with<counter<Adder, IsVolatile>>;

template <typename Adder = striped_long_adder>
using volatile_counter_ptr = ref_ptr<counter<Adder, true>>;

using concurrent_volatile_counter_ptr = counter_ptr<concurrent_long_adder, true>;

template <typename Adder = striped_long_adder>
using volatile_counter_prototype = metric_prototype_with<counter<Adder, true>>;

#define KTH_PERCENTILE(prefix, kth) prefix##kth
#define KTH_PERCENTILE_TYPE(kth) KTH_PERCENTILE(P, kth)
#define ENUM_KTH_PERCENTILE_TYPE(qualifier, kth) qualifier KTH_PERCENTILE_TYPE(kth)
#define KTH_PERCENTILE_NAME(kth) KTH_PERCENTILE(p, kth)

#define ENUM_REG_WITH_KTH_PERCENTILE_TYPE(kth)                                                     \
    ENUM_REG_WITH_CUSTOM_NAME(ENUM_KTH_PERCENTILE_TYPE(kth_percentile_type::, kth),                \
                              KTH_PERCENTILE_NAME(kth))

struct kth_percentile_property
{
    std::string name;
    double decimal;
};

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)
#define STRINGIFY_KTH_PERCENTILE_NAME(kth) STRINGIFY(KTH_PERCENTILE_NAME(kth))
#define KTH_TO_DECIMAL(kth) 0.##kth
#define KTH_PERCENTILE_PROPERTY_LIST(kth)                                                          \
    {                                                                                              \
        STRINGIFY_KTH_PERCENTILE_NAME(kth), KTH_TO_DECIMAL(kth)                                    \
    }

// All supported kinds of kth percentiles. User can configure required kth percentiles for
// each percentile. Only configured kth percentiles will be computed. This can reduce CPU
// consumption.
#define ALL_KTH_PERCENTILE_TYPES(qualifier)                                                        \
    ENUM_KTH_PERCENTILE_TYPE(qualifier, 50)                                                        \
    , ENUM_KTH_PERCENTILE_TYPE(qualifier, 90), ENUM_KTH_PERCENTILE_TYPE(qualifier, 95),            \
        ENUM_KTH_PERCENTILE_TYPE(qualifier, 99), ENUM_KTH_PERCENTILE_TYPE(qualifier, 999)

enum class kth_percentile_type : size_t
{
    ALL_KTH_PERCENTILE_TYPES(),
    COUNT,
    INVALID,
};

// Support to load from configuration files for percentiles.
ENUM_BEGIN(kth_percentile_type, kth_percentile_type::INVALID)
ENUM_REG_WITH_KTH_PERCENTILE_TYPE(50)
ENUM_REG_WITH_KTH_PERCENTILE_TYPE(90)
ENUM_REG_WITH_KTH_PERCENTILE_TYPE(95)
ENUM_REG_WITH_KTH_PERCENTILE_TYPE(99)
ENUM_REG_WITH_KTH_PERCENTILE_TYPE(999)
ENUM_END(kth_percentile_type)

// Generate decimals from kth percentiles.
const std::vector<kth_percentile_property> kAllKthPercentiles = {KTH_PERCENTILE_PROPERTY_LIST(50),
                                                                 KTH_PERCENTILE_PROPERTY_LIST(90),
                                                                 KTH_PERCENTILE_PROPERTY_LIST(95),
                                                                 KTH_PERCENTILE_PROPERTY_LIST(99),
                                                                 KTH_PERCENTILE_PROPERTY_LIST(999)};

const std::set<kth_percentile_type> kAllKthPercentileTypes = {
    ALL_KTH_PERCENTILE_TYPES(kth_percentile_type::)};

inline std::string kth_percentile_to_name(const kth_percentile_type &type)
{
    auto index = static_cast<size_t>(type);
    CHECK_LT(index, kAllKthPercentiles.size());
    return kAllKthPercentiles[index].name;
}

inline size_t kth_percentile_to_nth_index(size_t size, size_t kth_index)
{
    CHECK_LT(kth_index, kAllKthPercentiles.size());
    auto decimal = kAllKthPercentiles[kth_index].decimal;
    // Since the kth percentile is the value that is greater than k percent of the data values after
    // ranking them (https://people.richland.edu/james/ictcm/2001/descriptive/helpposition.html),
    // compute the nth index by size * decimal rather than size * decimal - 1.
    return static_cast<size_t>(size * decimal);
}

inline size_t kth_percentile_to_nth_index(size_t size, kth_percentile_type type)
{
    return kth_percentile_to_nth_index(size, static_cast<size_t>(type));
}

// The percentile is a metric type that samples observations. The size of samples has an upper
// bound. Once the maximum size is reached, the earliest observations will be overwritten.
//
// On the other hand, kth percentiles, such as P50, P90, P95, P99, P999, will be calculated
// periodically over all samples. The kth percentiles which are calculated are configurable
// provided that they are of valid kth_percentile_type (i.e. in kAllKthPercentileTypes).
//
// The most common usage of percentile is latency, such as server-level and replica-level
// latencies. For example, if P99 latency is 10 ms, it means the latencies of 99% requests
// are less than 10 ms.
//
// The percentile is implemented by the finder for nth elements. Each kth percentile is firstly
// converted to nth index; then, find the element corresponding to the nth index.
template <typename T,
          typename NthElementFinder = stl_nth_element_finder<T>,
          typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
class percentile : public closeable_metric
{
public:
    using value_type = T;
    using size_type = typename NthElementFinder::size_type;

    void set(const value_type &val)
    {
        const auto index = _tail.fetch_add(1, std::memory_order_relaxed);
        _samples.get()[index & (_sample_size - 1)] = val;
    }

    // If `type` is not configured, it will return false with zero value stored in `val`;
    // otherwise, it will always return true with the value corresponding to `type`.
    bool get(kth_percentile_type type, value_type &val) const
    {
        const auto index = static_cast<size_t>(type);
        CHECK_LT(index, static_cast<size_t>(kth_percentile_type::COUNT));

        val = value(index);
        return _kth_percentile_bitset.test(index);
    }

    // The snapshot collected has following json format:
    // {
    //     "name": "<metric_name>",
    //     "p50": ...,
    //     "p90": ...,
    //     "p95": ...,
    //     ...
    // }
    // where "name" is the name of the percentile in string type, with each configured kth
    // percentile followed, such as "p50", "p90", "p95", etc. All of them are in numeric types
    // (i.e. integral or floating-point type, determined by `value_type`).
    void take_snapshot(metric_json_writer &writer, const metric_filters &filters) override
    {
        writer.StartObject();

        encode_prototype(writer, filters);

        for (size_t i = 0; i < static_cast<size_t>(kth_percentile_type::COUNT); ++i) {
            if (!_kth_percentile_bitset.test(i)) {
                continue;
            }

            encode(writer, kAllKthPercentiles[i].name, value(i), filters);
        }

        writer.EndObject();
    }

    bool timer_enabled() const { return !!_timer; }

    uint64_t get_initial_delay_ms() const
    {
        return timer_enabled() ? _timer->get_initial_delay_ms() : 0;
    }

    static const size_type kDefaultSampleSize = 4096;

protected:
    // interval_ms is the interval between the computations for percentiles. Its unit is
    // milliseconds. It's suggested that interval_ms should be near the period between pulls
    // from or pushes to the monitoring system.
    // TODO(wangdan): we can also support constructing percentiles from the parameters in
    // the configuration file.
    percentile(const metric_prototype *prototype,
               uint64_t interval_ms = 10000,
               const std::set<kth_percentile_type> &kth_percentiles = kAllKthPercentileTypes,
               size_type sample_size = kDefaultSampleSize)
        : closeable_metric(prototype),
          _sample_size(sample_size),
          _last_real_sample_size(0),
          _samples(cacheline_aligned_alloc_array<value_type>(sample_size, value_type{})),
          _tail(0),
          _kth_percentile_bitset(),
          _full_nth_elements(static_cast<size_t>(kth_percentile_type::COUNT)),
          _nth_element_finder(),
          _timer()
    {
        CHECK(_sample_size > 0 && (_sample_size & (_sample_size - 1)) == 0,
              "sample_sizes should be > 0 and power of 2");
        CHECK(_samples, "");

        for (const auto &kth : kth_percentiles) {
            _kth_percentile_bitset.set(static_cast<size_t>(kth));
        }

        for (size_type i = 0; i < _full_nth_elements.size(); ++i) {
            _full_nth_elements[i].store(value_type{}, std::memory_order_relaxed);
        }

#ifdef DSN_MOCK_TEST
        if (interval_ms == 0) {
            // Timer is disabled.
            return;
        }
#else
        CHECK_GT(interval_ms, 0);
#endif

        // Increment ref count of percentile, since it will be referenced by timer.
        // This will extend the lifetime of percentile and prevent from heap-use-after-free
        // error.
        //
        // The ref count will be decremented at the moment when the percentile will
        // never be used by timer, which means the percentile can be destructed safely.
        // See on_close() for details which is registered in timer and will be called
        // back once close() is invoked.
        add_ref();
        _timer.reset(new metric_timer(
            interval_ms,
            std::bind(&percentile<value_type, NthElementFinder>::find_nth_elements, this),
            std::bind(&percentile<value_type, NthElementFinder>::on_close, this)));
    }

    virtual ~percentile() = default;

private:
    using nth_container_type = typename NthElementFinder::nth_container_type;

    friend class metric_entity;
    friend class ref_ptr<percentile<value_type, NthElementFinder>>;

    virtual void close() override
    {
        if (_timer) {
            _timer->close();
        }
    }

    virtual void wait() override
    {
        if (_timer) {
            _timer->wait();
        }
    }

    void on_close()
    {
        // This will be called back after timer is closed, which means the percentile is
        // no longer needed by timer and can be destructed safely.
        release_ref();
    }

    value_type value(size_t index) const
    {
        return _full_nth_elements[index].load(std::memory_order_relaxed);
    }

    void find_nth_elements()
    {
        size_type real_sample_size = std::min(static_cast<size_type>(_tail.load()), _sample_size);
        if (real_sample_size == 0) {
            // No need to find since there has not been any sample yet.
            return;
        }

        // If the size of samples changes, the nth indexs should be updated.
        if (real_sample_size != _last_real_sample_size) {
            set_real_nths(real_sample_size);
            _last_real_sample_size = real_sample_size;
        }

        // Find nth elements.
        std::vector<T> array(real_sample_size);
        std::copy(_samples.get(), _samples.get() + real_sample_size, array.begin());
        _nth_element_finder(array.begin(), array.begin(), array.end());

        // Store nth elements.
        const auto &elements = _nth_element_finder.elements();
        for (size_t i = 0, next = 0; i < static_cast<size_t>(kth_percentile_type::COUNT); ++i) {
            if (!_kth_percentile_bitset.test(i)) {
                continue;
            }
            _full_nth_elements[i].store(elements[next++], std::memory_order_relaxed);
        }
    }

    void set_real_nths(size_type real_sample_size)
    {
        nth_container_type nths;
        for (size_t i = 0; i < static_cast<size_t>(kth_percentile_type::COUNT); ++i) {
            if (!_kth_percentile_bitset.test(i)) {
                continue;
            }

            auto size = static_cast<size_t>(real_sample_size);
            auto nth = static_cast<size_type>(kth_percentile_to_nth_index(size, i));
            nths.push_back(nth);
        }

        _nth_element_finder.set_nths(nths);
    }

    const size_type _sample_size;
    size_type _last_real_sample_size;
    cacheline_aligned_ptr<value_type> _samples;
    std::atomic<uint64_t> _tail; // use unsigned int to avoid running out of bound
    std::bitset<static_cast<size_t>(kth_percentile_type::COUNT)> _kth_percentile_bitset;
    std::vector<std::atomic<value_type>> _full_nth_elements;
    NthElementFinder _nth_element_finder;

    std::unique_ptr<metric_timer> _timer;

    DISALLOW_COPY_AND_ASSIGN(percentile);
};

template <typename T,
          typename NthElementFinder = stl_nth_element_finder<T>,
          typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
using percentile_ptr = ref_ptr<percentile<T, NthElementFinder>>;

template <typename T,
          typename NthElementFinder = stl_nth_element_finder<T>,
          typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
using percentile_prototype = metric_prototype_with<percentile<T, NthElementFinder>>;

template <typename T,
          typename NthElementFinder = floating_stl_nth_element_finder<T>,
          typename = typename std::enable_if<std::is_floating_point<T>::value>::type>
using floating_percentile = percentile<T, NthElementFinder>;

template <typename T,
          typename NthElementFinder = floating_stl_nth_element_finder<T>,
          typename = typename std::enable_if<std::is_floating_point<T>::value>::type>
using floating_percentile_ptr = ref_ptr<floating_percentile<T, NthElementFinder>>;

template <typename T,
          typename NthElementFinder = floating_stl_nth_element_finder<T>,
          typename = typename std::enable_if<std::is_floating_point<T>::value>::type>
using floating_percentile_prototype =
    metric_prototype_with<floating_percentile<T, NthElementFinder>>;

} // namespace dsn
