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

#include <chrono>
#include <sstream>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "percentile_utils.h"
#include "utils/rand.h"

namespace dsn {

class my_gauge : public metric
{
public:
    int64_t value() { return _value; }

    void take_snapshot(json::JsonWriter &, const metric_filters &) override {}

protected:
    explicit my_gauge(const metric_prototype *prototype) : metric(prototype), _value(0) {}

    my_gauge(const metric_prototype *prototype, int64_t value) : metric(prototype), _value(value) {}

    virtual ~my_gauge() = default;

private:
    friend class metric_entity;
    friend class ref_ptr<my_gauge>;

    int64_t _value;

    DISALLOW_COPY_AND_ASSIGN(my_gauge);
};

using my_gauge_prototype = metric_prototype_with<my_gauge>;
using my_gauge_ptr = ref_ptr<my_gauge>;

} // namespace dsn

#define METRIC_DEFINE_my_gauge(entity_type, name, unit, desc, ...)                                 \
    ::dsn::my_gauge_prototype METRIC_##name(                                                       \
        {#entity_type, dsn::metric_type::kGauge, #name, unit, desc, ##__VA_ARGS__})

METRIC_DEFINE_entity(my_server);
METRIC_DEFINE_entity(my_table);
METRIC_DEFINE_entity(my_replica);

METRIC_DEFINE_my_gauge(my_server,
                       my_server_latency,
                       dsn::metric_unit::kMicroSeconds,
                       "a server-level latency for test");
METRIC_DEFINE_my_gauge(my_server,
                       my_server_duration,
                       dsn::metric_unit::kSeconds,
                       "a server-level duration for test");

METRIC_DEFINE_my_gauge(my_replica,
                       my_replica_latency,
                       dsn::metric_unit::kNanoSeconds,
                       "a replica-level latency for test");
METRIC_DEFINE_my_gauge(my_replica,
                       my_replica_duration,
                       dsn::metric_unit::kMilliSeconds,
                       "a replica-level duration for test");

METRIC_DEFINE_gauge_int64(my_server,
                          test_gauge_int64,
                          dsn::metric_unit::kMilliSeconds,
                          "a server-level gauge of int64 type for test");

METRIC_DEFINE_gauge_double(my_server,
                           test_gauge_double,
                           dsn::metric_unit::kSeconds,
                           "a server-level gauge of double type for test");

METRIC_DEFINE_counter(my_server,
                      test_counter,
                      dsn::metric_unit::kRequests,
                      "a server-level counter for test");

METRIC_DEFINE_concurrent_counter(my_server,
                                 test_concurrent_counter,
                                 dsn::metric_unit::kRequests,
                                 "a server-level concurrent_counter for test");

METRIC_DEFINE_volatile_counter(my_server,
                               test_volatile_counter,
                               dsn::metric_unit::kRequests,
                               "a server-level volatile_counter for test");

METRIC_DEFINE_concurrent_volatile_counter(my_server,
                                          test_concurrent_volatile_counter,
                                          dsn::metric_unit::kRequests,
                                          "a server-level concurrent_volatile_counter for test");

METRIC_DEFINE_percentile_int64(my_server,
                               test_percentile_int64,
                               dsn::metric_unit::kNanoSeconds,
                               "a server-level percentile of int64 type for test");

METRIC_DEFINE_percentile_double(my_server,
                                test_percentile_double,
                                dsn::metric_unit::kNanoSeconds,
                                "a server-level percentile of double type for test");

namespace dsn {

TEST(metrics_test, create_entity)
{
    // Test cases:
    // - create an entity by instantiate(id) without any attribute
    // - create another entity by instantiate(id, attrs) without any attribute
    // - create an entity with an attribute
    // - create another entity with an attribute
    // - create an entity with 2 attributes
    // - create another entity with 2 attributes
    struct test_case
    {
        metric_entity_prototype *prototype;
        std::string type_name;
        std::string entity_id;
        metric_entity::attr_map entity_attrs;
        bool use_attrs_arg_if_empty;
    } tests[] = {{&METRIC_ENTITY_my_server, "my_server", "server_1", {}, false},
                 {&METRIC_ENTITY_my_server, "my_server", "server_2", {}, true},
                 {&METRIC_ENTITY_my_table, "my_table", "test_1", {{"table", "test_1"}}, true},
                 {&METRIC_ENTITY_my_table, "my_table", "test_2", {{"table", "test_2"}}, true},
                 {&METRIC_ENTITY_my_replica,
                  "my_replica",
                  "1.2",
                  {{"table", "test_1"}, {"partition", "2"}},
                  true},
                 {&METRIC_ENTITY_my_replica,
                  "my_replica",
                  "2.5",
                  {{"table", "test_2"}, {"partition", "5"}},
                  true}};

    metric_registry::entity_map entities;
    for (const auto &test : tests) {
        ASSERT_EQ(test.prototype->name(), test.type_name);

        metric_entity_ptr entity;
        if (test.entity_attrs.empty() && !test.use_attrs_arg_if_empty) {
            entity = test.prototype->instantiate(test.entity_id);
        } else {
            entity = test.prototype->instantiate(test.entity_id, test.entity_attrs);
        }

        auto id = entity->id();
        ASSERT_EQ(id, test.entity_id);

        auto attrs = entity->attributes();
        ASSERT_NE(attrs.find("entity"), attrs.end());
        ASSERT_EQ(attrs["entity"], test.type_name);
        ASSERT_EQ(attrs.size(), test.entity_attrs.size() + 1);
        ASSERT_EQ(attrs.erase("entity"), 1);
        ASSERT_EQ(attrs, test.entity_attrs);

        ASSERT_EQ(entities.find(test.entity_id), entities.end());
        entities[test.entity_id] = entity;
    }

    ASSERT_EQ(metric_registry::instance().entities(), entities);
}

TEST(metrics_test, recreate_entity)
{
    // Test cases:
    // - add an attribute to an emtpy map
    // - add another attribute to a single-element map
    // - remove an attribute from the map
    // - remove the only attribute from the map
    struct test_case
    {
        metric_entity::attr_map entity_attrs;
    } tests[] = {
        {{{"name", "test"}}}, {{{"name", "test"}, {"id", "2"}}}, {{{"name", "test"}}}, {{{}}}};

    const std::string entity_id("test");
    auto expected_entity = METRIC_ENTITY_my_table.instantiate(entity_id);

    for (const auto &test : tests) {
        // the pointer of entity should be kept unchanged
        auto entity = METRIC_ENTITY_my_table.instantiate(entity_id, test.entity_attrs);
        ASSERT_EQ(entity, expected_entity);

        // the attributes will be updated
        auto attrs = entity->attributes();
        ASSERT_EQ(attrs.erase("entity"), 1);
        ASSERT_EQ(attrs, test.entity_attrs);
    }
}

TEST(metrics_test, create_metric)
{
    auto my_server_entity = METRIC_ENTITY_my_server.instantiate("server_3");
    auto my_replica_entity =
        METRIC_ENTITY_my_replica.instantiate("3.7", {{"table", "test_3"}, {"partition", "7"}});

    // Test cases:
    // - create an metric without any argument by an entity
    // - create an metric with an argument by an entity
    // - create an metric with an argument by another entity
    // - create an metric without any argument by another entity
    struct test_case
    {
        my_gauge_prototype *prototype;
        metric_entity_ptr entity;
        bool use_default_value;
        int64_t value;
    } tests[] = {{&METRIC_my_server_latency, my_server_entity, true, 0},
                 {&METRIC_my_server_duration, my_server_entity, false, 10},
                 {&METRIC_my_replica_latency, my_replica_entity, false, 100},
                 {&METRIC_my_replica_duration, my_replica_entity, true, 0}};

    using entity_map = std::unordered_map<metric_entity *, metric_entity::metric_map>;

    entity_map expected_entities;
    for (const auto &test : tests) {
        my_gauge_ptr my_metric;
        if (test.use_default_value) {
            my_metric = test.prototype->instantiate(test.entity);
        } else {
            my_metric = test.prototype->instantiate(test.entity, test.value);
        }

        ASSERT_EQ(my_metric->value(), test.value);

        auto iter = expected_entities.find(test.entity.get());
        if (iter == expected_entities.end()) {
            expected_entities[test.entity.get()] = {{test.prototype, my_metric}};
        } else {
            iter->second[test.prototype] = my_metric;
        }
    }

    entity_map actual_entities;
    auto entities = metric_registry::instance().entities();
    for (const auto &entity : entities) {
        if (expected_entities.find(entity.second.get()) != expected_entities.end()) {
            actual_entities[entity.second.get()] = entity.second->metrics();
        }
    }

    ASSERT_EQ(actual_entities, expected_entities);
}

TEST(metrics_test, recreate_metric)
{
    auto my_server_entity = METRIC_ENTITY_my_server.instantiate("server_4");

    auto my_metric = METRIC_my_server_latency.instantiate(my_server_entity, 5);
    ASSERT_EQ(my_metric->value(), 5);

    auto new_metric = METRIC_my_server_latency.instantiate(my_server_entity, 10);
    ASSERT_EQ(my_metric->value(), 5);
}

TEST(metrics_test, gauge_int64)
{
    // Test cases:
    // - create a gauge of int64 type without initial value, then increase
    // - create a gauge of int64 type without initial value, then decrease
    // - create a gauge of int64 type with initial value, then increase
    // - create a gauge of int64 type with initial value, then decrease
    struct test_case
    {
        std::string entity_id;
        bool use_default_value;
        int64_t initial_value;
        int64_t new_value;
    } tests[] = {{"server_5", true, 0, 5},
                 {"server_6", true, 0, -5},
                 {"server_7", false, 10, 100},
                 {"server_8", false, 100, 10}};

    for (const auto &test : tests) {
        auto my_server_entity = METRIC_ENTITY_my_server.instantiate(test.entity_id);

        gauge_ptr<int64_t> my_metric;
        if (test.use_default_value) {
            my_metric = METRIC_test_gauge_int64.instantiate(my_server_entity);
        } else {
            my_metric = METRIC_test_gauge_int64.instantiate(my_server_entity, test.initial_value);
        }

        ASSERT_EQ(my_metric->value(), test.initial_value);

        my_metric->set(test.new_value);
        ASSERT_EQ(my_metric->value(), test.new_value);

        auto metrics = my_server_entity->metrics();
        ASSERT_EQ(metrics[&METRIC_test_gauge_int64].get(), static_cast<metric *>(my_metric.get()));

        ASSERT_EQ(my_metric->prototype(),
                  static_cast<const metric_prototype *>(&METRIC_test_gauge_int64));
    }
}

TEST(metrics_test, gauge_double)
{
    // Test cases:
    // - create a gauge of double type without initial value, then increase
    // - create a gauge of double type without initial value, then decrease
    // - create a gauge of double type with initial value, then increase
    // - create a gauge of double type with initial value, then decrease
    struct test_case
    {
        std::string entity_id;
        bool use_default_value;
        double initial_value;
        double new_value;
    } tests[] = {{"server_9", true, 0.0, 5.278},
                 {"server_10", true, 0.0, -5.278},
                 {"server_11", false, 10.756, 100.128},
                 {"server_12", false, 100.128, 10.756}};

    for (const auto &test : tests) {
        auto my_server_entity = METRIC_ENTITY_my_server.instantiate(test.entity_id);

        gauge_ptr<double> my_metric;
        if (test.use_default_value) {
            my_metric = METRIC_test_gauge_double.instantiate(my_server_entity);
        } else {
            my_metric = METRIC_test_gauge_double.instantiate(my_server_entity, test.initial_value);
        }

        ASSERT_DOUBLE_EQ(my_metric->value(), test.initial_value);

        my_metric->set(test.new_value);
        ASSERT_DOUBLE_EQ(my_metric->value(), test.new_value);

        auto metrics = my_server_entity->metrics();
        ASSERT_EQ(metrics[&METRIC_test_gauge_double].get(), static_cast<metric *>(my_metric.get()));

        ASSERT_EQ(my_metric->prototype(),
                  static_cast<const metric_prototype *>(&METRIC_test_gauge_double));
    }
}

void execute(int64_t num_threads, std::function<void(int)> runner)
{
    std::vector<std::thread> threads;
    for (int64_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([i, &runner]() { runner(i); });
    }
    for (auto &t : threads) {
        t.join();
    }
}

template <typename MetricPtr>
void increment_by(std::integral_constant<bool, true>, MetricPtr &my_metric, int64_t x)
{
    my_metric->increment_by(x);
}

template <typename MetricPtr>
void increment_by(std::integral_constant<bool, false>, MetricPtr &my_metric, int64_t x)
{
    // If x is positive, metric will be increased; otherwise, the metric will be decreased.
    my_metric->decrement_by(-x);
}

template <bool IsIncrement, typename MetricPtr>
void run_increment_by(MetricPtr &my_metric,
                      int64_t base_value,
                      int64_t num_operations,
                      int64_t num_threads,
                      int64_t &result,
                      bool allow_negative = true)
{
    std::vector<int64_t> deltas;
    int64_t n = num_operations * num_threads;
    deltas.reserve(n);

    int64_t expected_value = base_value;
    for (int64_t i = 0; i < n; ++i) {
        auto delta = static_cast<int64_t>(dsn::rand::next_u64(1000000));
        if (allow_negative && delta % 3 == 0) {
            delta = -delta;
        }
        expected_value += delta;
        deltas.push_back(delta);
    }

    execute(num_threads, [num_operations, &my_metric, &deltas](int64_t tid) mutable {
        for (int64_t i = 0; i < num_operations; ++i) {
            auto delta = deltas[tid * num_operations + i];
            increment_by(std::integral_constant<bool, IsIncrement>{}, my_metric, delta);
        }
    });
    ASSERT_EQ(my_metric->value(), expected_value);
    result = expected_value;
}

template <typename MetricPtr>
void run_increment(MetricPtr &my_metric,
                   int64_t base_value,
                   int64_t num_operations,
                   int64_t num_threads,
                   int64_t &result)
{
    execute(num_threads, [num_operations, &my_metric](int) mutable {
        for (int64_t i = 0; i < num_operations; ++i) {
            my_metric->increment();
        }
    });

    int64_t expected_value = base_value + num_operations * num_threads;
    ASSERT_EQ(my_metric->value(), expected_value);
    result = expected_value;
}

template <typename MetricPtr>
void run_decrement(MetricPtr &my_metric,
                   int64_t base_value,
                   int64_t num_operations,
                   int64_t num_threads,
                   int64_t &result)
{
    execute(num_threads, [num_operations, &my_metric](int) mutable {
        for (int64_t i = 0; i < num_operations; ++i) {
            my_metric->decrement();
        }
    });

    int64_t expected_value = base_value - num_operations * num_threads;
    ASSERT_EQ(my_metric->value(), expected_value);
    result = expected_value;
}

void run_gauge_increment_cases(dsn::gauge_prototype<int64_t> *prototype, int64_t num_threads)
{
    // Test cases:
    // - test the gauge with small-scale computations
    // - test the gauge with large-scale computations
    struct test_case
    {
        std::string entity_id;
        int64_t increments_by;
        int64_t decrements_by;
        int64_t increments;
        int64_t decrements;
    } tests[] = {{"server_13", 100, 100, 1000, 1000},
                 {"server_14", 1000000, 1000000, 10000000, 10000000}};

    for (const auto &test : tests) {
        auto my_server_entity = METRIC_ENTITY_my_server.instantiate(test.entity_id);

        auto my_metric = prototype->instantiate(my_server_entity);

        int64_t value = 0;
        ASSERT_EQ(my_metric->value(), value);
        run_increment_by<true>(my_metric, value, test.increments_by, num_threads, value);
        run_increment_by<false>(my_metric, value, test.decrements_by, num_threads, value);
        run_increment(my_metric, value, test.increments, num_threads, value);
        run_decrement(my_metric, value, test.decrements, num_threads, value);

        // Reset to 0 since this metric could be used again
        my_metric->set(0);
        ASSERT_EQ(my_metric->value(), 0);
    }
}

void run_gauge_increment_cases(dsn::gauge_prototype<int64_t> *prototype)
{
    // Do single-threaded tests
    run_gauge_increment_cases(prototype, 1);

    // Do multi-threaded tests
    run_gauge_increment_cases(prototype, 4);
}

TEST(metrics_test, gauge_increment) { run_gauge_increment_cases(&METRIC_test_gauge_int64); }

template <typename Adder>
void run_counter_cases(dsn::counter_prototype<Adder> *prototype, int64_t num_threads)
{
    // Test cases:
    // - test the counter with small-scale computations
    // - test the counter with large-scale computations
    struct test_case
    {
        std::string entity_id;
        int64_t increments_by;
        int64_t increments;
    } tests[] = {{"server_15", 100, 1000}, {"server_16", 1000000, 10000000}};

    for (const auto &test : tests) {
        auto my_server_entity = METRIC_ENTITY_my_server.instantiate(test.entity_id);

        auto my_metric = prototype->instantiate(my_server_entity);

        int64_t value = 0;
        ASSERT_EQ(my_metric->value(), value);
        run_increment_by<true>(my_metric, value, test.increments_by, num_threads, value, false);
        run_increment(my_metric, value, test.increments, num_threads, value);

        my_metric->reset();
        ASSERT_EQ(my_metric->value(), 0);

        auto metrics = my_server_entity->metrics();
        ASSERT_EQ(metrics[prototype].get(), static_cast<metric *>(my_metric.get()));

        ASSERT_EQ(my_metric->prototype(), static_cast<const metric_prototype *>(prototype));
    }
}

template <typename Adder>
void run_counter_cases(dsn::counter_prototype<Adder> *prototype)
{
    // Do single-threaded tests
    run_counter_cases(prototype, 1);

    // Do multi-threaded tests
    run_counter_cases(prototype, 4);
}

TEST(metrics_test, counter)
{
    // Test both kinds of counter
    run_counter_cases<striped_long_adder>(&METRIC_test_counter);
    run_counter_cases<concurrent_long_adder>(&METRIC_test_concurrent_counter);
}

template <typename Adder>
void run_volatile_counter_write_and_read(dsn::volatile_counter_ptr<Adder> &my_metric,
                                         int64_t num_operations,
                                         int64_t num_threads_write,
                                         int64_t num_threads_read)
{
    std::vector<int64_t> deltas;
    int64_t n = num_operations * num_threads_write;
    deltas.reserve(n);

    int64_t expected_value = 0;
    for (int64_t i = 0; i < n; ++i) {
        auto delta = static_cast<int64_t>(dsn::rand::next_u64(1000000));
        expected_value += delta;
        deltas.push_back(delta);
    }

    auto results = new_cacheline_aligned_int64_array(static_cast<uint32_t>(num_threads_read));
    std::vector<std::atomic_bool> completed(num_threads_write);
    for (int64_t i = 0; i < num_threads_write; ++i) {
        completed[i].store(false);
    }

    ASSERT_EQ(my_metric->value(), 0);

    execute(num_threads_write + num_threads_read,
            [num_operations, num_threads_write, &my_metric, &deltas, &results, &completed](
                int64_t tid) mutable {
                if (tid < num_threads_write) {
                    for (int64_t i = 0; i < num_operations; ++i) {
                        my_metric->increment_by(deltas[tid * num_operations + i]);
                    }
                    completed[tid].store(true);
                } else {
                    bool done = false;
                    do {
                        int64_t i = 0;
                        for (; i < num_threads_write && completed[i].load(); ++i) {
                        }
                        if (i >= num_threads_write) {
                            // All of the increment threads have finished, thus the loop can
                            // be broken after the last time the value is fetched.
                            done = true;
                        }

                        auto value = my_metric->value();
                        if (value == 0) {
                            // If zero is fetched, it's likely that recently the counter is
                            // not updated frequently. Thus yield and try for the next time.
                            std::this_thread::yield();
                        } else {
                            auto r = results.get();
                            r[tid - num_threads_write]._value += value;
                        }
                    } while (!done);
                }
            });

    int64_t value = 0;
    for (int64_t i = 0; i < num_threads_read; ++i) {
        value += results.get()[i]._value.load();
    }
    ASSERT_EQ(value, expected_value);
    ASSERT_EQ(my_metric->value(), 0);
}

template <typename Adder>
void run_volatile_counter_cases(dsn::volatile_counter_prototype<Adder> *prototype,
                                int64_t num_threads_write,
                                int64_t num_threads_read)
{
    // Test cases:
    // - test the volatile counter with small-scale computations
    // - test the volatile counter with large-scale computations
    struct test_case
    {
        std::string entity_id;
        int64_t num_operations;
    } tests[] = {{"server_17", 5000}, {"server_18", 5000000}};

    for (const auto &test : tests) {
        auto my_server_entity = METRIC_ENTITY_my_server.instantiate(test.entity_id);

        auto my_metric = prototype->instantiate(my_server_entity);

        run_volatile_counter_write_and_read(
            my_metric, test.num_operations, num_threads_write, num_threads_read);

        auto metrics = my_server_entity->metrics();
        ASSERT_EQ(metrics[prototype].get(), static_cast<metric *>(my_metric.get()));

        ASSERT_EQ(my_metric->prototype(), static_cast<const metric_prototype *>(prototype));
    }
}

template <typename Adder>
void run_volatile_counter_cases(dsn::volatile_counter_prototype<Adder> *prototype)
{
    // Write with single thread and read with single thread
    run_volatile_counter_cases(prototype, 1, 1);

    // Write with multiple threads and read with single thread
    run_volatile_counter_cases(prototype, 2, 1);

    // Write with single thread and read with multiple threads
    run_volatile_counter_cases(prototype, 1, 2);

    // Write with multiple threads and read with multiple threads
    run_volatile_counter_cases(prototype, 4, 2);
}

TEST(metrics_test, volatile_counter)
{
    // Test both kinds of volatile counter
    run_volatile_counter_cases<striped_long_adder>(&METRIC_test_volatile_counter);
    run_volatile_counter_cases<concurrent_long_adder>(&METRIC_test_concurrent_volatile_counter);
}

template <typename T, typename Prototype, typename Checker>
void run_percentile(const metric_entity_ptr &my_entity,
                    const Prototype &prototype,
                    const std::vector<T> &data,
                    size_t num_preload,
                    uint64_t interval_ms,
                    uint64_t exec_ms,
                    const std::set<kth_percentile_type> &kth_percentiles,
                    size_t sample_size,
                    size_t num_threads,
                    const std::vector<T> &expected_elements,
                    Checker checker)
{
    CHECK_GT(num_threads, 0);
    CHECK(data.size() <= sample_size && data.size() % num_threads == 0,
          "Invalid arguments, data_size={}, sample_size={}, num_threads={}",
          data.size(),
          sample_size,
          num_threads);

    auto my_metric = prototype.instantiate(my_entity, interval_ms, kth_percentiles, sample_size);

    // Preload zero in current thread.
    for (size_t i = 0; i < num_preload; ++i) {
        my_metric->set(0);
    }

    // Load other data in each spawned thread evenly.
    const size_t num_operations = data.size() / num_threads;
    execute(static_cast<int64_t>(num_threads),
            [num_operations, &my_metric, &data](int64_t tid) mutable {
                for (size_t i = 0; i < num_operations; ++i) {
                    my_metric->set(data[static_cast<size_t>(tid) * num_operations + i]);
                }
            });

    // Wait a while in order to finish computing all percentiles.
    std::this_thread::sleep_for(
        std::chrono::milliseconds(my_metric->get_initial_delay_ms() + interval_ms + exec_ms));

    // Check if actual elements of kth percentiles are equal to the expected ones.
    std::vector<T> actual_elements;
    for (const auto &kth : kAllKthPercentileTypes) {
        T value;
        if (kth_percentiles.find(kth) == kth_percentiles.end()) {
            ASSERT_FALSE(my_metric->get(kth, value));
            checker(value, 0);
        } else {
            ASSERT_TRUE(my_metric->get(kth, value));
            actual_elements.push_back(value);
        }
    }
    checker(actual_elements, expected_elements);

    // Check if this percentile is included in the entity.
    auto metrics = my_entity->metrics();
    ASSERT_EQ(metrics[&prototype].get(), static_cast<metric *>(my_metric.get()));

    // Check if the prototype is referenced by this percentile.
    ASSERT_EQ(my_metric->prototype(), static_cast<const metric_prototype *>(&prototype));
}

template <typename T, typename Prototype, typename CaseGenerator, typename Checker>
void run_percentile_cases(const Prototype &prototype)
{
    using value_type = T;
    const auto p50 = kth_percentile_type::P50;
    const auto p90 = kth_percentile_type::P90;
    const auto p99 = kth_percentile_type::P99;

    // Test cases:
    // - input none of sample with none of kth percentile
    // - input 1 sample with none of kth percentile
    // - input 1 sample with 1 kth percentile
    // - input 1 sample with 2 kth percentiles
    // - input 1 sample with all kth percentiles
    // - input 1 sample with 1 kth percentile, capacity of 2
    // - input 1 sample with 2 kth percentiles, capacity of 2
    // - input 1 sample with all kth percentiles, capacity of 2
    // - input 2 samples with 1 kth percentile
    // - input 2 samples with 2 kth percentiles
    // - input 2 samples with all kth percentiles
    // - input 10 samples with 1 kth percentile, capacity of 16
    // - input 10 samples with 2 kth percentiles, capacity of 16
    // - input 10 samples with all kth percentiles, capacity of 16
    // - input 10 samples with 1 kth percentile by 2 threads, capacity of 16
    // - input 10 samples with 2 kth percentiles by 2 threads, capacity of 16
    // - input 10 samples with all kth percentiles by 2 threads, capacity of 16
    // - input 16 samples with 1 kth percentile
    // - input 16 samples with 2 kth percentiles
    // - input 16 samples with all kth percentiles
    // - input 16 samples with 1 kth percentile by 2 threads
    // - input 16 samples with 2 kth percentiles by 2 threads
    // - input 16 samples with all kth percentiles by 2 threads
    // - preload 5 samples and input 16 samples with 1 kth percentile by 2 threads
    // - preload 5 samples and input 16 samples with 2 kth percentiles by 2 threads
    // - preload 5 samples and input 16 samples with all kth percentiles by 2 threads
    // - input 2000 samples with 1 kth percentile, capacity of 4096
    // - input 2000 samples with 2 kth percentiles, capacity of 4096
    // - input 2000 samples with all kth percentiles, capacity of 4096
    // - input 2000 samples with 1 kth percentile by 4 threads, capacity of 4096
    // - input 2000 samples with 2 kth percentiles by 4 threads, capacity of 4096
    // - input 2000 samples with all kth percentiles by 4 threads, capacity of 4096
    // - input 4096 samples with 1 kth percentile, capacity of 4096
    // - input 4096 samples with 2 kth percentiles, capacity of 4096
    // - input 4096 samples with all kth percentiles, capacity of 4096
    // - input 4096 samples with 1 kth percentile by 4 threads, capacity of 4096
    // - input 4096 samples with 2 kth percentiles by 4 threads, capacity of 4096
    // - input 4096 samples with all kth percentiles by 4 threads, capacity of 4096
    // - preload 5 input 4096 samples with 1 kth percentile by 4 threads, capacity of 4096
    // - preload 5 input 4096 samples with 2 kth percentiles by 4 threads, capacity of 4096
    // - preload 5 input 4096 samples with all kth percentiles by 4 threads, capacity of 4096
    struct test_case
    {
        std::string entity_id;
        size_t data_size;
        value_type initial_value;
        uint64_t range_size;
        size_t num_preload;
        uint64_t interval_ms;
        uint64_t exec_ms;
        const std::set<kth_percentile_type> kth_percentiles;
        size_t sample_size;
        size_t num_threads;
    } tests[] = {{"server_19", 0, 0, 2, 0, 50, 10, {}, 1, 1},
                 {"server_20", 1, 0, 2, 0, 50, 10, {}, 1, 1},
                 {"server_21", 1, 0, 2, 0, 50, 10, {p90}, 1, 1},
                 {"server_22", 1, 0, 2, 0, 50, 10, {p50, p99}, 1, 1},
                 {"server_23", 1, 0, 2, 0, 50, 10, kAllKthPercentileTypes, 1, 1},
                 {"server_24", 1, 0, 2, 0, 50, 10, {p90}, 2, 1},
                 {"server_25", 1, 0, 2, 0, 50, 10, {p50, p99}, 2, 1},
                 {"server_26", 1, 0, 2, 0, 50, 10, kAllKthPercentileTypes, 2, 1},
                 {"server_27", 2, 0, 2, 0, 50, 10, {p90}, 2, 1},
                 {"server_28", 2, 0, 2, 0, 50, 10, {p50, p99}, 2, 1},
                 {"server_29", 2, 0, 2, 0, 50, 10, kAllKthPercentileTypes, 2, 1},
                 {"server_30", 10, 0, 2, 0, 50, 10, {p90}, 16, 1},
                 {"server_31", 10, 0, 2, 0, 50, 10, {p50, p99}, 16, 1},
                 {"server_32", 10, 0, 2, 0, 50, 10, kAllKthPercentileTypes, 16, 1},
                 {"server_33", 10, 0, 2, 0, 50, 10, {p90}, 16, 2},
                 {"server_34", 10, 0, 2, 0, 50, 10, {p50, p99}, 16, 2},
                 {"server_35", 10, 0, 2, 0, 50, 10, kAllKthPercentileTypes, 16, 2},
                 {"server_36", 16, 0, 2, 0, 50, 10, {p90}, 16, 1},
                 {"server_37", 16, 0, 2, 0, 50, 10, {p50, p99}, 16, 1},
                 {"server_38", 16, 0, 2, 0, 50, 10, kAllKthPercentileTypes, 16, 1},
                 {"server_39", 16, 0, 2, 0, 50, 10, {p90}, 16, 2},
                 {"server_40", 16, 0, 2, 0, 50, 10, {p50, p99}, 16, 2},
                 {"server_41", 16, 0, 2, 0, 50, 10, kAllKthPercentileTypes, 16, 2},
                 {"server_42", 16, 0, 2, 5, 50, 10, {p90}, 16, 2},
                 {"server_43", 16, 0, 2, 5, 50, 10, {p50, p99}, 16, 2},
                 {"server_44", 16, 0, 2, 5, 50, 10, kAllKthPercentileTypes, 16, 2},
                 {"server_45", 2000, 0, 5, 0, 50, 10, {p90}, 4096, 1},
                 {"server_46", 2000, 0, 5, 0, 50, 10, {p50, p99}, 4096, 1},
                 {"server_47", 2000, 0, 5, 0, 50, 10, kAllKthPercentileTypes, 4096, 1},
                 {"server_48", 2000, 0, 5, 0, 50, 10, {p90}, 4096, 4},
                 {"server_49", 2000, 0, 5, 0, 50, 10, {p50, p99}, 4096, 4},
                 {"server_50", 2000, 0, 5, 0, 50, 10, kAllKthPercentileTypes, 4096, 4},
                 {"server_51", 4096, 0, 5, 0, 50, 10, {p90}, 4096, 1},
                 {"server_52", 4096, 0, 5, 0, 50, 10, {p50, p99}, 4096, 1},
                 {"server_53", 4096, 0, 5, 0, 50, 10, kAllKthPercentileTypes, 4096, 1},
                 {"server_54", 4096, 0, 5, 0, 50, 10, {p90}, 4096, 4},
                 {"server_55", 4096, 0, 5, 0, 50, 10, {p50, p99}, 4096, 4},
                 {"server_56", 4096, 0, 5, 0, 50, 10, kAllKthPercentileTypes, 4096, 4},
                 {"server_57", 4096, 0, 5, 5, 50, 10, {p90}, 4096, 4},
                 {"server_58", 4096, 0, 5, 5, 50, 10, {p50, p99}, 4096, 4},
                 {"server_59", 4096, 0, 5, 5, 50, 10, kAllKthPercentileTypes, 4096, 4}};

    for (const auto &test : tests) {
        auto my_server_entity = METRIC_ENTITY_my_server.instantiate(test.entity_id);

        CaseGenerator generator(
            test.data_size, test.initial_value, test.range_size, test.kth_percentiles);

        std::vector<value_type> data;
        std::vector<value_type> expected_elements;
        generator(data, expected_elements);

        run_percentile<value_type, Prototype, Checker>(my_server_entity,
                                                       prototype,
                                                       data,
                                                       test.num_preload,
                                                       test.interval_ms,
                                                       test.exec_ms,
                                                       test.kth_percentiles,
                                                       test.sample_size,
                                                       test.num_threads,
                                                       expected_elements,
                                                       Checker());
    }
}

template <typename T>
class integral_checker
{
public:
    void operator()(const T &actual_element, const T &expected_element) const
    {
        ASSERT_EQ(actual_element, expected_element);
    }

    void operator()(const std::vector<T> &actual_elements,
                    const std::vector<T> &expected_elements) const
    {
        ASSERT_EQ(actual_elements, expected_elements);
    }
};

TEST(metrics_test, percentile_int64)
{
    using value_type = int64_t;
    run_percentile_cases<value_type,
                         percentile_prototype<value_type>,
                         integral_percentile_case_generator<value_type>,
                         integral_checker<value_type>>(METRIC_test_percentile_int64);
}

template <typename T>
class floating_checker
{
public:
    void operator()(const T &actual_element, const T &expected_element) const
    {
        ASSERT_DOUBLE_EQ(actual_element, expected_element);
    }

    void operator()(const std::vector<T> &actual_elements,
                    const std::vector<T> &expected_elements) const
    {
        ASSERT_EQ(actual_elements.size(), expected_elements.size());
        for (size_t i = 0; i < expected_elements.size(); ++i) {
            ASSERT_DOUBLE_EQ(actual_elements[i], expected_elements[i]);
        }
    }
};

TEST(metrics_test, percentile_double)
{
    using value_type = double;
    run_percentile_cases<value_type,
                         floating_percentile_prototype<value_type>,
                         floating_percentile_case_generator<value_type>,
                         floating_checker<value_type>>(METRIC_test_percentile_double);
}

std::string take_snapshot_and_get_json_string(metric *m, const metric_filters &filters)
{
    std::stringstream out;
    rapidjson::OStreamWrapper wrapper(out);
    json::JsonWriter writer(wrapper);

    m->take_snapshot(writer, filters);

    return out.str();
}

template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
using metric_value_map = std::map<std::string, T>;

template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
void check_prototype_and_extract_value_map_from_json_string(
    metric *my_metric,
    const std::string &json_string,
    const bool is_integral,
    const metric_fields_type &expected_metric_fields,
    metric_value_map<T> &value_map)
{
    rapidjson::Document doc;
    rapidjson::ParseResult result = doc.Parse(json_string.c_str());
    ASSERT_FALSE(result.IsError());

    metric_fields_type actual_metric_fields;

    // The json format for each metric should be an object.
    ASSERT_TRUE(doc.IsObject());
    for (const auto &elem : doc.GetObject()) {
        // Each metric name must be a string.
        ASSERT_TRUE(elem.name.IsString());

        if (elem.value.IsString()) {
            // Must be a field of metric prototype.
            if (std::strcmp(elem.name.GetString(), kMetricTypeField.c_str()) == 0) {
                ASSERT_STREQ(elem.value.GetString(),
                             enum_to_string(my_metric->prototype()->type()));
            } else if (std::strcmp(elem.name.GetString(), kMetricNameField.c_str()) == 0) {
                ASSERT_STREQ(elem.value.GetString(), my_metric->prototype()->name().data());
            } else if (std::strcmp(elem.name.GetString(), kMetricUnitField.c_str()) == 0) {
                ASSERT_STREQ(elem.value.GetString(),
                             enum_to_string(my_metric->prototype()->unit()));
            } else if (std::strcmp(elem.name.GetString(), kMetricDescField.c_str()) == 0) {
                ASSERT_STREQ(elem.value.GetString(), my_metric->prototype()->description().data());
            } else {
                ASSERT_TRUE(false);
            }
        } else {
            // Must be a field of metric value.
            T value;
            if (is_integral) {
                ASSERT_TRUE(elem.value.IsInt64());
                value = elem.value.GetInt64();
            } else {
                ASSERT_TRUE(elem.value.IsDouble());
                value = elem.value.GetDouble();
            }
            value_map[elem.name.GetString()] = value;
        }

        actual_metric_fields.emplace(elem.name.GetString());
    }

    // Check if the fields of metric prototype have been provided to the client as expected.
    ASSERT_EQ(actual_metric_fields, expected_metric_fields);
}

// Take snapshot as json format, then decode to a value map.
template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
void generate_metric_value_map(metric *my_metric,
                               const bool is_integral,
                               const metric_filters &filters,
                               const metric_fields_type &expected_metric_fields,
                               metric_value_map<T> &value_map)
{
    auto json_string = take_snapshot_and_get_json_string(my_metric, filters);
    check_prototype_and_extract_value_map_from_json_string(
        my_metric, json_string, is_integral, expected_metric_fields, value_map);
}

template <typename T, typename = typename std::enable_if<std::is_integral<T>::value>::type>
void compare_integral_metric_value_map(const metric_value_map<T> &actual_value_map,
                                       const metric_value_map<T> &expected_value_map)
{
    ASSERT_EQ(actual_value_map, expected_value_map);
}

template <typename T, typename = typename std::enable_if<std::is_floating_point<T>::value>::type>
void compare_floating_metric_value_map(const metric_value_map<T> &actual_value_map,
                                       const metric_value_map<T> &expected_value_map)
{
    ASSERT_EQ(actual_value_map.size(), expected_value_map.size());

    auto actual_iter = actual_value_map.begin();
    auto expected_iter = expected_value_map.begin();
    for (; actual_iter != actual_value_map.end() && expected_iter != expected_value_map.end();
         ++actual_iter, ++expected_iter) {
        ASSERT_EQ(actual_iter->first, expected_iter->first);
        ASSERT_DOUBLE_EQ(actual_iter->second, expected_iter->second);
    }
}

#define TEST_METRIC_SNAPSHOT_WITH_SINGLE_VALUE(metric_prototype,                                   \
                                               updater,                                            \
                                               value_type,                                         \
                                               is_integral,                                        \
                                               metric_fields,                                      \
                                               expected_metric_fields,                             \
                                               value_map_comparator)                               \
    do {                                                                                           \
        auto my_server_entity = METRIC_ENTITY_my_server.instantiate(test.entity_id);               \
        auto my_metric = metric_prototype.instantiate(my_server_entity);                           \
        my_metric->updater(test.expected_value);                                                   \
                                                                                                   \
        metric_filters filters;                                                                    \
        filters.with_metric_fields = metric_fields;                                                \
                                                                                                   \
        metric_value_map<value_type> expected_value_map;                                           \
        if (expected_metric_fields.find(kMetricSingleValueField) !=                                \
            expected_metric_fields.end()) {                                                        \
            expected_value_map[kMetricSingleValueField] = test.expected_value;                     \
        }                                                                                          \
                                                                                                   \
        metric_value_map<value_type> actual_value_map;                                             \
        generate_metric_value_map(                                                                 \
            my_metric.get(), is_integral, filters, expected_metric_fields, actual_value_map);      \
                                                                                                   \
        value_map_comparator(actual_value_map, expected_value_map);                                \
    } while (0)

const metric_fields_type kAllPrototypeMetricFields = {
    kMetricTypeField, kMetricNameField, kMetricUnitField, kMetricDescField};

metric_fields_type get_all_single_value_metric_fields()
{
    auto fields = kAllPrototypeMetricFields;
    fields.insert(kMetricSingleValueField);
    return fields;
}

// Test cases:
// - with_metric_fields is empty
// - with_metric_fields has a field of prototype that exists
// - with_metric_fields has a field of value that exists
// - with_metric_fields has 2 fields of prototype that exist
// - with_metric_fields has 2 fields that exist where there is a field of prototype and a field
// of value
// - with_metric_fields has 3 fields that exist where there is 2 fields of prototype and a field
// of value
// - with_metric_fields has all fields which exist
// - with_metric_fields has a field that does not exist
// - with_metric_fields has 2 fields both of which does not exist
// - with_metric_fields has a field that does not exist and another field of prototype that
// exists
// - with_metric_fields has a field that does not exist and another field of value that exists
// - with_metric_fields has a field that does not exist and another 2 fields of prototype that
// exists
// - with_metric_fields has a field that does not exist and another 2 fields that exist where
// there is a field of prototype and a field of value
// - with_metric_fields has a field that does not exist and another 3 fields that exist where
// there is 2 fields of prototype and a field of value
// - with_metric_fields has 2 fields that does not exist and another 3 fields that exist where
// there is 2 fields of prototype and a field of value
#define RUN_CASES_WITH_SINGLE_VALUE_SNAPSHOT(                                                      \
    metric_prototype, updater, value_type, is_integral, value, value_map_comparator)               \
    do {                                                                                           \
        static const metric_fields_type kAllSingleValueMetricFields =                              \
            get_all_single_value_metric_fields();                                                  \
        struct test_case                                                                           \
        {                                                                                          \
            std::string entity_id;                                                                 \
            value_type expected_value;                                                             \
            metric_fields_type with_metric_fields;                                                 \
            metric_fields_type expected_metric_fields;                                             \
        } tests[] = {                                                                              \
            {"server_60", value, {}, kAllSingleValueMetricFields},                                 \
            {"server_61", value, {kMetricNameField}, {kMetricNameField}},                          \
            {"server_62", value, {kMetricSingleValueField}, {kMetricSingleValueField}},            \
            {"server_63",                                                                          \
             value,                                                                                \
             {kMetricNameField, kMetricDescField},                                                 \
             {kMetricNameField, kMetricDescField}},                                                \
            {"server_64",                                                                          \
             value,                                                                                \
             {kMetricNameField, kMetricSingleValueField},                                          \
             {kMetricNameField, kMetricSingleValueField}},                                         \
            {"server_65",                                                                          \
             value,                                                                                \
             {kMetricTypeField, kMetricNameField, kMetricSingleValueField},                        \
             {kMetricTypeField, kMetricNameField, kMetricSingleValueField}},                       \
            {"server_66", value, kAllSingleValueMetricFields, kAllSingleValueMetricFields},        \
            {"server_67", value, {"field_not_exist"}, {}},                                         \
            {"server_68", value, {"field_not_exist", "another_field_not_exist"}, {}},              \
            {"server_69", value, {"field_not_exist", kMetricNameField}, {kMetricNameField}},       \
            {"server_70",                                                                          \
             value,                                                                                \
             {"field_not_exist", kMetricSingleValueField},                                         \
             {kMetricSingleValueField}},                                                           \
            {"server_71",                                                                          \
             value,                                                                                \
             {"field_not_exist", kMetricTypeField, kMetricUnitField},                              \
             {kMetricTypeField, kMetricUnitField}},                                                \
            {"server_72",                                                                          \
             value,                                                                                \
             {"field_not_exist", kMetricDescField, kMetricSingleValueField},                       \
             {kMetricDescField, kMetricSingleValueField}},                                         \
            {"server_73",                                                                          \
             value,                                                                                \
             {"field_not_exist", kMetricTypeField, kMetricNameField, kMetricSingleValueField},     \
             {kMetricTypeField, kMetricNameField, kMetricSingleValueField}},                       \
            {"server_74",                                                                          \
             value,                                                                                \
             {"field_not_exist",                                                                   \
              "another_field_not_exist",                                                           \
              kMetricUnitField,                                                                    \
              kMetricDescField,                                                                    \
              kMetricSingleValueField},                                                            \
             {kMetricUnitField, kMetricDescField, kMetricSingleValueField}}};                      \
                                                                                                   \
        for (const auto &test : tests) {                                                           \
            TEST_METRIC_SNAPSHOT_WITH_SINGLE_VALUE(metric_prototype,                               \
                                                   updater,                                        \
                                                   value_type,                                     \
                                                   is_integral,                                    \
                                                   test.with_metric_fields,                        \
                                                   test.expected_metric_fields,                    \
                                                   value_map_comparator);                          \
        }                                                                                          \
    } while (0);

#define RUN_CASES_WITH_GAUGE_SNAPSHOT(                                                             \
    metric_prototype, value_type, is_integral, value, value_map_comparator)                        \
    RUN_CASES_WITH_SINGLE_VALUE_SNAPSHOT(                                                          \
        metric_prototype, set, value_type, is_integral, value, value_map_comparator)

TEST(metrics_test, take_snapshot_gauge_int64)
{
    RUN_CASES_WITH_GAUGE_SNAPSHOT(
        METRIC_test_gauge_int64, int64_t, true, 5, compare_integral_metric_value_map);
}

TEST(metrics_test, take_snapshot_gauge_double)
{
    RUN_CASES_WITH_GAUGE_SNAPSHOT(
        METRIC_test_gauge_double, double, false, 6.789, compare_floating_metric_value_map);
}

#define RUN_CASES_WITH_COUNTER_SNAPSHOT(metric_prototype)                                          \
    RUN_CASES_WITH_SINGLE_VALUE_SNAPSHOT(                                                          \
        metric_prototype, increment_by, int64_t, true, 10, compare_integral_metric_value_map)

TEST(metrics_test, take_snapshot_counter) { RUN_CASES_WITH_COUNTER_SNAPSHOT(METRIC_test_counter); }

TEST(metrics_test, take_snapshot_concurrent_counter)
{
    RUN_CASES_WITH_COUNTER_SNAPSHOT(METRIC_test_concurrent_counter);
}

TEST(metrics_test, take_snapshot_volatile_counter)
{
    RUN_CASES_WITH_COUNTER_SNAPSHOT(METRIC_test_volatile_counter);
}

TEST(metrics_test, take_snapshot_concurrent_volatile_counter)
{
    RUN_CASES_WITH_COUNTER_SNAPSHOT(METRIC_test_concurrent_volatile_counter);
}

// Set to percentile metric with values output by case generator, and generate the expected
// value map.
template <typename MetricType, typename CaseGenerator>
void generate_metric_value_map(MetricType *my_metric,
                               CaseGenerator &generator,
                               const uint64_t interval_ms,
                               const uint64_t exec_ms,
                               const std::set<kth_percentile_type> &kth_percentiles,
                               const metric_fields_type &expected_metric_fields,
                               metric_value_map<typename MetricType::value_type> &value_map)
{
    using value_type = typename MetricType::value_type;

    std::vector<value_type> data;
    std::vector<value_type> values;
    generator(data, values);
    ASSERT_EQ(kth_percentiles.size(), values.size());

    for (const auto &elem : data) {
        my_metric->set(elem);
    }

    // Wait a while in order that computations for all percentiles can be finished.
    std::this_thread::sleep_for(
        std::chrono::milliseconds(my_metric->get_initial_delay_ms() + interval_ms + exec_ms));

    auto value = values.begin();
    for (const auto &type : kth_percentiles) {
        auto name = kth_percentile_to_name(type);
        // Only add the chosen fields to the expected value map.
        if (expected_metric_fields.find(name) != expected_metric_fields.end()) {
            value_map[name] = *value;
        }
        ++value;
    }
}

#define TEST_METRIC_SNAPSHOT_WITH_PERCENTILE(metric_prototype,                                     \
                                             case_generator,                                       \
                                             is_integral,                                          \
                                             metric_fields,                                        \
                                             expected_metric_fields,                               \
                                             value_map_comparator)                                 \
    do {                                                                                           \
        using value_type = typename case_generator::value_type;                                    \
                                                                                                   \
        const uint64_t interval_ms = 50;                                                           \
        const auto kth_percentiles = kAllKthPercentileTypes;                                       \
        const size_t sample_size = 4096;                                                           \
        const size_t data_size = 4096;                                                             \
        const uint64_t exec_ms = 10;                                                               \
                                                                                                   \
        auto my_server_entity = METRIC_ENTITY_my_server.instantiate(test.entity_id);               \
        auto my_metric = metric_prototype.instantiate(                                             \
            my_server_entity, interval_ms, kth_percentiles, sample_size);                          \
                                                                                                   \
        metric_filters filters;                                                                    \
        filters.with_metric_fields = metric_fields;                                                \
                                                                                                   \
        case_generator generator(                                                                  \
            data_size, value_type() /* initial_value */, 5 /* range_size */, kth_percentiles);     \
                                                                                                   \
        metric_value_map<value_type> expected_value_map;                                           \
        generate_metric_value_map(my_metric.get(),                                                 \
                                  generator,                                                       \
                                  interval_ms,                                                     \
                                  exec_ms,                                                         \
                                  kth_percentiles,                                                 \
                                  expected_metric_fields,                                          \
                                  expected_value_map);                                             \
                                                                                                   \
        metric_value_map<value_type> actual_value_map;                                             \
        generate_metric_value_map(                                                                 \
            my_metric.get(), is_integral, filters, expected_metric_fields, actual_value_map);      \
                                                                                                   \
        value_map_comparator(actual_value_map, expected_value_map);                                \
    } while (0)

metric_fields_type get_all_kth_percentile_fields()
{
    auto fields = kAllPrototypeMetricFields;
    for (const auto &kth : kAllKthPercentiles) {
        fields.insert(kth.name);
    }
    return fields;
}

// Test cases:
// - with_metric_fields is empty
// - with_metric_fields has a field of prototype that exists
// - with_metric_fields has a field of value that exists
// - with_metric_fields has 2 fields of prototype that exist
// - with_metric_fields has 2 fields of value that exist
// - with_metric_fields has 2 fields that exist where there is a field of prototype and a field
// of value
// - with_metric_fields has 3 fields that exist where there is 2 fields of prototype and a field
// of value
// - with_metric_fields has 3 fields that exist where there is a field of prototype and 2 fields
// of value
// - with_metric_fields has 4 fields that exist where there is 2 fields of prototype and 2 fields
// of value
// - with_metric_fields has all fields that exist
// - with_metric_fields has a field that does not exist
// - with_metric_fields has 2 fields both of which does not exist
// - with_metric_fields has a field that does not exist and another field of prototype that
// exists
// - with_metric_fields has a field that does not exist and another field of value that
// exists
// - with_metric_fields has a field that does not exist and another 2 fields of prototype that
// exist
// - with_metric_fields has a field that does not exist and another 2 fields of value that
// exist
// - with_metric_fields has a field that does not exist and another 2 fields that exist where
// there is a field of prototype and a field of value
// - with_metric_fields has a field that does not exist and another 3 fields that exist where
// there is 2 fields of prototype and a field of value
// - with_metric_fields has a field that does not exist and another 3 fields that exist where
// there is a field of prototype and 2 fields of value
// - with_metric_fields has a field that does not exist and another 4 fields that exist where
// there is 2 fields of prototype and 2 fields of value
// - with_metric_fields has 2 fields that does not exist and another 4 fields that exist where
// there is 2 fields of prototype and 2 fields of value
#define RUN_CASES_WITH_PERCENTILE_SNAPSHOT(                                                        \
    metric_prototype, case_generator, is_integral, value_map_comparator)                           \
    do {                                                                                           \
        static const metric_fields_type kAllKthPercentileFields = get_all_kth_percentile_fields(); \
                                                                                                   \
        struct test_case                                                                           \
        {                                                                                          \
            std::string entity_id;                                                                 \
            metric_fields_type with_metric_fields;                                                 \
            metric_fields_type expected_metric_fields;                                             \
        } tests[] = {                                                                              \
            {"server_60", {}, kAllKthPercentileFields},                                            \
            {"server_61", {kMetricNameField}, {kMetricNameField}},                                 \
            {"server_62", {"p999"}, {"p999"}},                                                     \
            {"server_63",                                                                          \
             {kMetricTypeField, kMetricDescField},                                                 \
             {kMetricTypeField, kMetricDescField}},                                                \
            {"server_64", {"p50", "p999"}, {"p50", "p999"}},                                       \
            {"server_65", {kMetricUnitField, "p99"}, {kMetricUnitField, "p99"}},                   \
            {"server_66",                                                                          \
             {kMetricNameField, kMetricUnitField, "p99"},                                          \
             {kMetricNameField, kMetricUnitField, "p99"}},                                         \
            {"server_67", {kMetricDescField, "p95", "p999"}, {kMetricDescField, "p95", "p999"}},   \
            {"server_68",                                                                          \
             {kMetricTypeField, kMetricNameField, "p90", "p99"},                                   \
             {kMetricTypeField, kMetricNameField, "p90", "p99"}},                                  \
            {"server_69", kAllKthPercentileFields, kAllKthPercentileFields},                       \
            {"server_70", {"field_not_exist"}, {}},                                                \
            {"server_71", {"field_not_exist", "another_field_not_exist"}, {}},                     \
            {"server_72", {"file_not_exist", kMetricTypeField}, {kMetricTypeField}},               \
            {"server_73", {"file_not_exist", "p99"}, {"p99"}},                                     \
            {"server_74",                                                                          \
             {"field_not_exist", kMetricUnitField, kMetricDescField},                              \
             {kMetricUnitField, kMetricDescField}},                                                \
            {"server_75", {"file_not_exist", "p50", "p99"}, {"p50", "p99"}},                       \
            {"server_76",                                                                          \
             {"field_not_exist", kMetricNameField, "p999"},                                        \
             {kMetricNameField, "p999"}},                                                          \
            {"server_77",                                                                          \
             {"field_not_exist", kMetricTypeField, kMetricUnitField, "p99"},                       \
             {kMetricTypeField, kMetricUnitField, "p99"}},                                         \
            {"server_78",                                                                          \
             {"field_not_exist", kMetricDescField, "p90", "p999"},                                 \
             {kMetricDescField, "p90", "p999"}},                                                   \
            {"server_79",                                                                          \
             {"field_not_exist", kMetricNameField, kMetricUnitField, "p95", "p99"},                \
             {kMetricNameField, kMetricUnitField, "p95", "p99"}},                                  \
            {"server_80",                                                                          \
             {"field_not_exist",                                                                   \
              "another_field_not_exist",                                                           \
              kMetricTypeField,                                                                    \
              kMetricDescField,                                                                    \
              "p50",                                                                               \
              "p999"},                                                                             \
             {kMetricTypeField, kMetricDescField, "p50", "p999"}}};                                \
                                                                                                   \
        for (const auto &test : tests) {                                                           \
            TEST_METRIC_SNAPSHOT_WITH_PERCENTILE(metric_prototype,                                 \
                                                 case_generator,                                   \
                                                 is_integral,                                      \
                                                 test.with_metric_fields,                          \
                                                 test.expected_metric_fields,                      \
                                                 value_map_comparator);                            \
        }                                                                                          \
    } while (0)

TEST(metrics_test, take_snapshot_percentile_int64)
{
    RUN_CASES_WITH_PERCENTILE_SNAPSHOT(METRIC_test_percentile_int64,
                                       integral_percentile_case_generator<int64_t>,
                                       true,
                                       compare_integral_metric_value_map);
}

TEST(metrics_test, take_snapshot_percentile_double)
{
    RUN_CASES_WITH_PERCENTILE_SNAPSHOT(METRIC_test_percentile_double,
                                       floating_percentile_case_generator<double>,
                                       false,
                                       compare_floating_metric_value_map);
}

} // namespace dsn
