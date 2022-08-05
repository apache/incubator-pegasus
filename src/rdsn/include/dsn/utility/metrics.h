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
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio/deadline_timer.hpp>

#include <dsn/c/api_utilities.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/alloc.h>
#include <dsn/utility/autoref_ptr.h>
#include <dsn/utility/casts.h>
#include <dsn/utility/enum_helper.h>
#include <dsn/utility/long_adder.h>
#include <dsn/utility/nth_element.h>
#include <dsn/utility/ports.h>
#include <dsn/utility/singleton.h>
#include <dsn/utility/string_view.h>
#include <dsn/utility/synchronize.h>

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

class metric_data_sink;
using metric_data_sink_ptr = ref_ptr<metric_data_sink>;

class metric_entity : public ref_counter
{
public:
    using attr_map = std::map<std::string, std::string>;
    using metric_map = std::unordered_map<const metric_prototype *, metric_ptr>;

    const std::string &id() const { return _id; }

    attr_map attributes() const;

    metric_map metrics() const;

    // args are the parameters that are used to construct the object of MetricType
    template <typename MetricType, typename... Args>
    ref_ptr<MetricType> find_or_create(const metric_prototype *prototype, Args &&... args)
    {
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

private:
    friend class metric_registry;
    friend class ref_ptr<metric_entity>;

    metric_entity(const std::string &id, attr_map &&attrs);

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

    void set_attributes(attr_map &&attrs);

    void collect_metrics(const std::vector<metric_data_sink_ptr> &sinks);

    const std::string _id;

    mutable utils::rw_lock_nr _lock;
    attr_map _attrs;
    metric_map _metrics;

    DISALLOW_COPY_AND_ASSIGN(metric_entity);
};

using metric_entity_ptr = ref_ptr<metric_entity>;

class metric_entity_prototype
{
public:
    explicit metric_entity_prototype(const char *name);
    ~metric_entity_prototype();

    const char *name() const { return _name; }

    // Create an entity with the given ID and attributes, if any.
    metric_entity_ptr instantiate(const std::string &id, metric_entity::attr_map attrs) const;
    metric_entity_ptr instantiate(const std::string &id) const;

private:
    const char *const _name;

    DISALLOW_COPY_AND_ASSIGN(metric_entity_prototype);
};

// `metric_timer` is a timer class that runs metric-related computations periodically, such as
// calculating percentile, collecting snapshots from all metrics to data sinks, etc.
//
// To be instantiated, it requires `interval_ms` at which computations are run, `on_exec`
// implementing computations, and `on_close` as the callback for close.
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
};

class metric_registry : public utils::singleton<metric_registry>
{
public:
    using entity_map = std::unordered_map<std::string, metric_entity_ptr>;

    entity_map entities() const;

private:
    friend class metric_entity_prototype;
    friend class utils::singleton<metric_registry>;
    friend class metrics_test;

    metric_registry();
    ~metric_registry();

    void on_close();

    template <typename MetricDataSink, typename... Args>
    ref_ptr<MetricDataSink> register_data_sink(Args &&... args)
    {
        static_assert(std::is_base_of<metric_data_sink, MetricDataSink>::value,
                      "not derived from metric_data_sink");

        utils::auto_write_lock l(_lock);
        ref_ptr<MetricDataSink> ptr(new MetricDataSink(std::forward<Args>(args)...));
        _sinks.push_back(ptr);
        return ptr;
    }

    void unregister_data_sinks()
    {
        utils::auto_write_lock l(_lock);
        std::vector<metric_data_sink_ptr>().swap(_sinks);
    }

    metric_entity_ptr find_or_create_entity(const std::string &id, metric_entity::attr_map &&attrs);

    void collect_metrics();

    mutable utils::rw_lock_nr _lock;
    std::vector<metric_data_sink_ptr> _sinks;
    entity_map _entities;
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
ENUM_REG(metric_type::kGauge)
ENUM_REG(metric_type::kCounter)
ENUM_REG(metric_type::kVolatileCounter)
ENUM_REG(metric_type::kPercentile)
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
ENUM_REG(metric_unit::kNanoSeconds)
ENUM_REG(metric_unit::kMicroSeconds)
ENUM_REG(metric_unit::kMilliSeconds)
ENUM_REG(metric_unit::kSeconds)
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

// A snapshot to the metric
class metric_snapshot
{
public:
    using value_type = double;
    using attr_map = std::map<std::string, std::string>;

    metric_snapshot() = default;

    metric_snapshot(const string_view &name, metric_type type, value_type value, attr_map &&attrs);

    virtual ~metric_snapshot() = default;

    const string_view &name() const { return _name; }

    metric_type type() const { return _type; }

    value_type value() const { return _value; }

    const attr_map &attrs() const { return _attrs; }

    std::string encode_attributes() const;
    static attr_map decode_attributes(const std::string &str);

    bool operator==(const metric_snapshot &rhs) const;

    bool operator!=(const metric_snapshot &rhs) const;

private:
    string_view _name;
    metric_type _type;
    value_type _value;

    // Additional attributes of the metric value, could be empty.
    // If a metric emits multiple snapshots per time, each snapshot
    // will be tagged with an attribute.
    attr_map _attrs;
};

class counter_snapshot : public metric_snapshot
{
public:
    counter_snapshot() = default;

    counter_snapshot(const string_view &name,
                     metric_type type,
                     value_type value,
                     attr_map &&attrs,
                     value_type increase);

    virtual ~counter_snapshot() = default;

    value_type increase() const { return _increase; }

    bool operator==(const counter_snapshot &rhs) const;

    bool operator!=(const counter_snapshot &rhs) const;

private:
    value_type _increase;
};

class metric_data_sink : public ref_counter
{
public:
    metric_data_sink() = default;
    virtual ~metric_data_sink() = default;

    virtual void iterate(const metric_snapshot &snapshot) = 0;
    virtual void iterate(const counter_snapshot &snapshot) = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(metric_data_sink);
};

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

protected:
    explicit metric(const metric_prototype *prototype);
    virtual ~metric() = default;

    virtual void take_snapshot(const std::vector<metric_data_sink_ptr> &sinks,
                               const metric_snapshot::attr_map &attrs) = 0;

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

    virtual void take_snapshot(const std::vector<metric_data_sink_ptr> &sinks,
                               const metric_snapshot::attr_map &attrs) override
    {
        for (auto &sink : sinks) {
            sink->iterate(metric_snapshot(prototype()->name(),
                                          prototype()->type(),
                                          static_cast<metric_snapshot::value_type>(value()),
                                          metric_snapshot::attr_map(attrs)));
        }
    }

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

    // NOTICE: x MUST be a non-negative integer.
    void increment_by(int64_t x)
    {
        dassert_f(x >= 0, "delta({}) by increment for counter must be a non-negative integer", x);
        _adder.increment_by(x);
    }

    void increment() { _adder.increment(); }

    void reset() { _adder.reset(); }

protected:
    counter(const metric_prototype *prototype)
        : metric(prototype), _adder(), _snapshot(metric_snapshot::value_type())
    {
    }

    virtual ~counter() = default;

    virtual void take_snapshot(const std::vector<metric_data_sink_ptr> &sinks,
                               const metric_snapshot::attr_map &attrs) override
    {
        auto old_value = _snapshot.load(std::memory_order_relaxed);
        auto new_value = value();
        for (auto &sink : sinks) {
            sink->iterate(
                counter_snapshot(prototype()->name(),
                                 prototype()->type(),
                                 static_cast<metric_snapshot::value_type>(new_value),
                                 metric_snapshot::attr_map(attrs),
                                 static_cast<metric_snapshot::value_type>(new_value - old_value)));
        }

        _snapshot.store(new_value, std::memory_order_relaxed);
    }

private:
    friend class metric_entity;
    friend class ref_ptr<counter<Adder, IsVolatile>>;

    long_adder_wrapper<Adder> _adder;
    std::atomic<metric_snapshot::value_type> _snapshot;

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

// All supported kinds of kth percentiles. User can configure required kth percentiles for
// each percentile. Only configured kth percentiles will be computed. This can reduce CPU
// consumption.
enum class kth_percentile_type : size_t
{
    P50,
    P90,
    P95,
    P99,
    P999,
    COUNT,
    INVALID
};

// Support to load from configuration files for percentiles.
ENUM_BEGIN(kth_percentile_type, kth_percentile_type::INVALID)
ENUM_REG(kth_percentile_type::P50)
ENUM_REG(kth_percentile_type::P90)
ENUM_REG(kth_percentile_type::P95)
ENUM_REG(kth_percentile_type::P99)
ENUM_REG(kth_percentile_type::P999)
ENUM_END(kth_percentile_type)

const std::vector<double> kKthDecimals = {0.5, 0.9, 0.95, 0.99, 0.999};
const std::vector<std::string> kKthLabels = {"50", "90", "95", "99", "999"};

inline size_t kth_percentile_to_nth_index(size_t size, size_t kth_index)
{
    auto decimal = kKthDecimals[kth_index];
    // Since the kth percentile is the value that is greater than k percent of the data values after
    // ranking them (https://people.richland.edu/james/ictcm/2001/descriptive/helpposition.html),
    // compute the nth index by size * decimal rather than size * decimal - 1.
    return static_cast<size_t>(size * decimal);
}

inline size_t kth_percentile_to_nth_index(size_t size, kth_percentile_type type)
{
    return kth_percentile_to_nth_index(size, static_cast<size_t>(type));
}

std::set<kth_percentile_type> get_all_kth_percentile_types();
const std::set<kth_percentile_type> kAllKthPercentileTypes = get_all_kth_percentile_types();

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
        dcheck_lt(index, static_cast<size_t>(kth_percentile_type::COUNT));

        val = value(index);
        return _kth_percentile_bitset.test(index);
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
        dassert(_sample_size > 0 && (_sample_size & (_sample_size - 1)) == 0,
                "sample_sizes should be > 0 and power of 2");

        dassert(_samples, "_samples should be valid pointer");

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
        dcheck_gt(interval_ms, 0);
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

    virtual void take_snapshot(const std::vector<metric_data_sink_ptr> &sinks,
                               const metric_snapshot::attr_map &attrs) override
    {
        for (size_t i = 0; i < static_cast<size_t>(kth_percentile_type::COUNT); ++i) {
            if (!_kth_percentile_bitset.test(i)) {
                continue;
            }
            for (auto &sink : sinks) {
                metric_snapshot::attr_map labels({{"p", kKthLabels[i]}});
                labels.insert(attrs.begin(), attrs.end());
                sink->iterate(metric_snapshot(prototype()->name(),
                                              prototype()->type(),
                                              static_cast<metric_snapshot::value_type>(value(i)),
                                              std::move(labels)));
            }
        }
    }

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
