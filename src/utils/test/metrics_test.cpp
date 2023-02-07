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
#include <thread>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "http/http_message_parser.h"
#include "percentile_utils.h"
#include "runtime/rpc/rpc_message.h"
#include "utils/blob.h"
#include "utils/rand.h"

namespace dsn {

DSN_DECLARE_uint64(entity_retirement_delay_ms);

class my_gauge : public metric
{
public:
    int64_t value() { return _value; }

    void take_snapshot(metric_json_writer &, const metric_filters &) override {}

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

// Dedicated entity for getting metrics by http service.
METRIC_DEFINE_entity(my_app);

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
                          test_server_gauge_int64,
                          dsn::metric_unit::kMilliSeconds,
                          "a server-level gauge of int64 type for test");

METRIC_DEFINE_gauge_int64(my_table,
                          test_table_gauge_int64,
                          dsn::metric_unit::kMilliSeconds,
                          "a table-level gauge of int64 type for test");

METRIC_DEFINE_gauge_int64(my_replica,
                          test_replica_gauge_int64,
                          dsn::metric_unit::kMilliSeconds,
                          "a replica-level gauge of int64 type for test");

METRIC_DEFINE_gauge_int64(my_app,
                          test_app_gauge_int64,
                          dsn::metric_unit::kMilliSeconds,
                          "an app-level gauge of int64 type for test");

METRIC_DEFINE_gauge_double(my_server,
                           test_server_gauge_double,
                           dsn::metric_unit::kSeconds,
                           "a server-level gauge of double type for test");

METRIC_DEFINE_counter(my_server,
                      test_server_counter,
                      dsn::metric_unit::kRequests,
                      "a server-level counter for test");

METRIC_DEFINE_counter(my_table,
                      test_table_counter,
                      dsn::metric_unit::kRequests,
                      "a table-level counter for test");

METRIC_DEFINE_counter(my_replica,
                      test_replica_counter,
                      dsn::metric_unit::kRequests,
                      "a replica-level counter for test");

METRIC_DEFINE_counter(my_app,
                      test_app_counter,
                      dsn::metric_unit::kRequests,
                      "an app-level counter for test");

METRIC_DEFINE_concurrent_counter(my_server,
                                 test_server_concurrent_counter,
                                 dsn::metric_unit::kRequests,
                                 "a server-level concurrent_counter for test");

METRIC_DEFINE_volatile_counter(my_server,
                               test_server_volatile_counter,
                               dsn::metric_unit::kRequests,
                               "a server-level volatile_counter for test");

METRIC_DEFINE_concurrent_volatile_counter(my_server,
                                          test_server_concurrent_volatile_counter,
                                          dsn::metric_unit::kRequests,
                                          "a server-level concurrent_volatile_counter for test");

METRIC_DEFINE_percentile_int64(my_server,
                               test_server_percentile_int64,
                               dsn::metric_unit::kNanoSeconds,
                               "a server-level percentile of int64 type for test");

METRIC_DEFINE_percentile_double(my_server,
                                test_server_percentile_double,
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
        ASSERT_STREQ(test.prototype->name(), test.type_name.c_str());

        metric_entity_ptr entity;
        if (test.entity_attrs.empty() && !test.use_attrs_arg_if_empty) {
            entity = test.prototype->instantiate(test.entity_id);
        } else {
            entity = test.prototype->instantiate(test.entity_id, test.entity_attrs);
        }

        auto id = entity->id();
        ASSERT_EQ(id, test.entity_id);

        auto attrs = entity->attributes();
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
            my_metric = METRIC_test_server_gauge_int64.instantiate(my_server_entity);
        } else {
            my_metric =
                METRIC_test_server_gauge_int64.instantiate(my_server_entity, test.initial_value);
        }

        ASSERT_EQ(my_metric->value(), test.initial_value);

        my_metric->set(test.new_value);
        ASSERT_EQ(my_metric->value(), test.new_value);

        auto metrics = my_server_entity->metrics();
        ASSERT_EQ(metrics[&METRIC_test_server_gauge_int64].get(),
                  static_cast<metric *>(my_metric.get()));

        ASSERT_EQ(my_metric->prototype(),
                  static_cast<const metric_prototype *>(&METRIC_test_server_gauge_int64));
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
            my_metric = METRIC_test_server_gauge_double.instantiate(my_server_entity);
        } else {
            my_metric =
                METRIC_test_server_gauge_double.instantiate(my_server_entity, test.initial_value);
        }

        ASSERT_DOUBLE_EQ(my_metric->value(), test.initial_value);

        my_metric->set(test.new_value);
        ASSERT_DOUBLE_EQ(my_metric->value(), test.new_value);

        auto metrics = my_server_entity->metrics();
        ASSERT_EQ(metrics[&METRIC_test_server_gauge_double].get(),
                  static_cast<metric *>(my_metric.get()));

        ASSERT_EQ(my_metric->prototype(),
                  static_cast<const metric_prototype *>(&METRIC_test_server_gauge_double));
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

TEST(metrics_test, gauge_increment) { run_gauge_increment_cases(&METRIC_test_server_gauge_int64); }

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
    run_counter_cases<striped_long_adder>(&METRIC_test_server_counter);
    run_counter_cases<concurrent_long_adder>(&METRIC_test_server_concurrent_counter);
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
    run_volatile_counter_cases<striped_long_adder>(&METRIC_test_server_volatile_counter);
    run_volatile_counter_cases<concurrent_long_adder>(
        &METRIC_test_server_concurrent_volatile_counter);
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
                         integral_checker<value_type>>(METRIC_test_server_percentile_int64);
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
                         floating_checker<value_type>>(METRIC_test_server_percentile_double);
}

template <typename T>
std::string take_snapshot_and_get_json_string(T *m, const metric_filters &filters)
{
    auto out_str = take_snapshot_as_json(m, filters);
    if (out_str.empty()) {
        std::cout << "The json string is empty." << std::endl;
    } else {
        std::cout << "The json string is: " << std::endl;
        std::cout << out_str << std::endl;
    }
    return out_str;
}

template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
using metric_value_map = std::map<std::string, T>;

template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
void check_prototype_and_extract_value_map_from_json_string(
    metric *my_metric,
    const std::string &json_string,
    const bool is_integral,
    const metric_filters::metric_fields_type &expected_metric_fields,
    metric_value_map<T> &value_map)
{
    rapidjson::Document doc;
    rapidjson::ParseResult result = doc.Parse(json_string.c_str());
    ASSERT_FALSE(result.IsError());

    metric_filters::metric_fields_type actual_metric_fields;

    // The json format for each metric should be an object.
    ASSERT_TRUE(doc.IsObject());
    for (const auto &elem : doc.GetObject()) {
        // Each name must be a string.
        ASSERT_TRUE(elem.name.IsString());

        if (elem.value.IsString()) {
            // Must be a field of metric prototype.
            if (kMetricTypeField == elem.name.GetString()) {
                ASSERT_STREQ(elem.value.GetString(),
                             enum_to_string(my_metric->prototype()->type()));
            } else if (kMetricNameField == elem.name.GetString()) {
                ASSERT_STREQ(elem.value.GetString(), my_metric->prototype()->name().data());
            } else if (kMetricUnitField == elem.name.GetString()) {
                ASSERT_STREQ(elem.value.GetString(),
                             enum_to_string(my_metric->prototype()->unit()));
            } else if (kMetricDescField == elem.name.GetString()) {
                ASSERT_STREQ(elem.value.GetString(), my_metric->prototype()->description().data());
            } else {
                ASSERT_TRUE(false) << "invalid field name: " << elem.name.GetString();
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
                               const metric_filters::metric_fields_type &expected_metric_fields,
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

const metric_filters::metric_fields_type kAllPrototypeMetricFields = {
    kMetricTypeField, kMetricNameField, kMetricUnitField, kMetricDescField};

metric_filters::metric_fields_type get_all_single_value_metric_fields()
{
    auto fields = kAllPrototypeMetricFields;
    fields.insert(kMetricSingleValueField);
    return fields;
}

const metric_filters::metric_fields_type kAllSingleValueMetricFields =
    get_all_single_value_metric_fields();

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
        struct test_case                                                                           \
        {                                                                                          \
            std::string entity_id;                                                                 \
            value_type expected_value;                                                             \
            metric_filters::metric_fields_type with_metric_fields;                                 \
            metric_filters::metric_fields_type expected_metric_fields;                             \
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
        METRIC_test_server_gauge_int64, int64_t, true, 5, compare_integral_metric_value_map);
}

TEST(metrics_test, take_snapshot_gauge_double)
{
    RUN_CASES_WITH_GAUGE_SNAPSHOT(
        METRIC_test_server_gauge_double, double, false, 6.789, compare_floating_metric_value_map);
}

#define RUN_CASES_WITH_COUNTER_SNAPSHOT(metric_prototype)                                          \
    RUN_CASES_WITH_SINGLE_VALUE_SNAPSHOT(                                                          \
        metric_prototype, increment_by, int64_t, true, 10, compare_integral_metric_value_map)

TEST(metrics_test, take_snapshot_counter)
{
    RUN_CASES_WITH_COUNTER_SNAPSHOT(METRIC_test_server_counter);
}

TEST(metrics_test, take_snapshot_concurrent_counter)
{
    RUN_CASES_WITH_COUNTER_SNAPSHOT(METRIC_test_server_concurrent_counter);
}

TEST(metrics_test, take_snapshot_volatile_counter)
{
    RUN_CASES_WITH_COUNTER_SNAPSHOT(METRIC_test_server_volatile_counter);
}

TEST(metrics_test, take_snapshot_concurrent_volatile_counter)
{
    RUN_CASES_WITH_COUNTER_SNAPSHOT(METRIC_test_server_concurrent_volatile_counter);
}

// Set to percentile metric with values output by case generator, and generate the expected
// value map.
template <typename MetricType, typename CaseGenerator>
void generate_metric_value_map(MetricType *my_metric,
                               CaseGenerator &generator,
                               const uint64_t interval_ms,
                               const uint64_t exec_ms,
                               const std::set<kth_percentile_type> &kth_percentiles,
                               const metric_filters::metric_fields_type &expected_metric_fields,
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

metric_filters::metric_fields_type get_all_kth_percentile_fields()
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
        static const metric_filters::metric_fields_type kAllKthPercentileFields =                  \
            get_all_kth_percentile_fields();                                                       \
                                                                                                   \
        struct test_case                                                                           \
        {                                                                                          \
            std::string entity_id;                                                                 \
            metric_filters::metric_fields_type with_metric_fields;                                 \
            metric_filters::metric_fields_type expected_metric_fields;                             \
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
    RUN_CASES_WITH_PERCENTILE_SNAPSHOT(METRIC_test_server_percentile_int64,
                                       integral_percentile_case_generator<int64_t>,
                                       true,
                                       compare_integral_metric_value_map);
}

TEST(metrics_test, take_snapshot_percentile_double)
{
    RUN_CASES_WITH_PERCENTILE_SNAPSHOT(METRIC_test_server_percentile_double,
                                       floating_percentile_case_generator<double>,
                                       false,
                                       compare_floating_metric_value_map);
}

const std::unordered_set<std::string> kAllMetricEntityFields = {kMetricEntityTypeField,
                                                                kMetricEntityIdField,
                                                                kMetricEntityAttrsField,
                                                                kMetricEntityMetricsField};

void check_entity_from_json_string(metric_entity *my_entity,
                                   const std::string &json_string,
                                   const std::string &expected_entity_type,
                                   const std::string &expected_entity_id,
                                   const metric_entity::attr_map &expected_entity_attrs,
                                   const std::unordered_set<std::string> &expected_entity_metrics)
{
    // `json_string` and `expected_entity_metrics` should be empty or non-empty simultaneously;
    // empty `json_string` means this entity is not selected by the filters.
    ASSERT_EQ(json_string.empty(), expected_entity_metrics.empty());
    if (json_string.empty()) {
        std::cout << "Empty json string means this entity is not selected by the filters."
                  << std::endl;
        return;
    }

    rapidjson::Document doc;
    rapidjson::ParseResult result = doc.Parse(json_string.c_str());
    ASSERT_FALSE(result.IsError());

    // Actual fields parsed from json string for each entity.
    std::unordered_set<std::string> actual_fields;

    // The json format for each entity should be an object.
    ASSERT_TRUE(doc.IsObject());
    for (const auto &elem : doc.GetObject()) {
        // Each name must be a string.
        ASSERT_TRUE(elem.name.IsString());

        if (kMetricEntityTypeField == elem.name.GetString()) {
            ASSERT_STREQ(elem.value.GetString(), expected_entity_type.c_str());
            ASSERT_STREQ(elem.value.GetString(), my_entity->prototype()->name());
        } else if (kMetricEntityIdField == elem.name.GetString()) {
            ASSERT_STREQ(elem.value.GetString(), expected_entity_id.c_str());
            ASSERT_STREQ(elem.value.GetString(), my_entity->id().c_str());
        } else if (kMetricEntityAttrsField == elem.name.GetString()) {
            ASSERT_TRUE(elem.value.IsObject());

            metric_entity::attr_map actual_entity_attrs;
            for (const auto &attr : elem.value.GetObject()) {
                // Each name must be a string.
                ASSERT_TRUE(attr.name.IsString());
                ASSERT_TRUE(attr.value.IsString());
                actual_entity_attrs.emplace(attr.name.GetString(), attr.value.GetString());
            }
            ASSERT_EQ(actual_entity_attrs, expected_entity_attrs);
            ASSERT_EQ(actual_entity_attrs, my_entity->attributes());
        } else if (kMetricEntityMetricsField == elem.name.GetString()) {
            ASSERT_TRUE(elem.value.IsArray());

            std::unordered_set<std::string> actual_entity_metrics;
            for (const auto &m : elem.value.GetArray()) {
                ASSERT_TRUE(m.IsObject());

                for (const auto &field : m.GetObject()) {
                    // Each name must be a string.
                    ASSERT_TRUE(field.name.IsString());
                    if (kMetricNameField == field.name.GetString()) {
                        ASSERT_TRUE(field.value.IsString());
                        actual_entity_metrics.emplace(field.value.GetString());
                    }
                }
            }

            ASSERT_EQ(actual_entity_metrics, expected_entity_metrics);
        } else {
            ASSERT_TRUE(false) << "invalid field name: " << elem.name.GetString();
        }

        actual_fields.emplace(elem.name.GetString());
    }

    ASSERT_EQ(actual_fields, kAllMetricEntityFields);
}

TEST(metrics_test, take_snapshot_entity)
{
    static const std::unordered_set<std::string> kAllServerEntityMetrics = {
        "test_server_gauge_int64", "test_server_counter"};
    static const std::unordered_set<std::string> kAllTableEntityMetrics = {"test_table_gauge_int64",
                                                                           "test_table_counter"};
    static const std::unordered_set<std::string> kAllReplicaEntityMetrics = {
        "test_replica_gauge_int64", "test_replica_counter"};

    // Test cases:
    // - both attributes and filters are empty
    // - entity has an attribute while filters are empty
    // - entity has 2 attributes while filters are empty
    // - entity has 2 attributes while filters are empty and metrics are empty
    // - filter has one matched entity type
    // - filter has one mismatched entity type
    // - filter has 2 entity types one of which is matched
    // - filter has 2 mismatched entity types
    // - filter has one matched entity id
    // - filter has one mismatched entity id
    // - filter has 2 entity ids one of which is matched
    // - filter has 2 mismatched entity ids
    // - entity has no attribute while filter has one mismatched entity attribute
    // - entity has no attribute while filter has 2 mismatched entity attributes
    // - entity has an attribute while filter has one matched entity attribute
    // - entity has an attribute while filter has one entity attribute whose key is mismatched
    // - entity has an attribute while filter has one entity attribute whose value is mismatched
    // - entity has an attribute while filter has one entity attribute whose key and value are
    // both mismatched
    // - entity has an attribute while filter has 2 entity attributes one of which is matched
    // - entity has an attribute while filter has 2 mismatched entity attributes
    // - entity has 2 attributes while filter has one matched entity attribute
    // - entity has 2 attributes while filter has one entity attribute whose key is mismatched
    // - entity has 2 attributes while filter has one entity attribute whose value is mismatched
    // - entity has 2 attributes while filter has one entity attribute whose key and value are
    // both mismatched
    // - entity has 2 attributes while filter has 2 matched entity attributes
    // - entity has 2 attributes while filter has 2 entity attributes one of which is matched
    // - entity has 2 attributes while filter has 2 mismatched entity attributes
    // - entity has no metrics while filter has one mismatched entity metrics
    // - entity has no metrics while filter has 2 mismatched entity metrics
    // - entity has an metric while filter has one matched entity metric
    // - entity has an metric while filter has one mismatched entity metric
    // - entity has an metric while filter has 2 entity metrics one of which is matched
    // - entity has an metric while filter has 2 mismatched entity metrics
    // - entity has 2 metrics while filter has one matched entity metric
    // - entity has 2 metrics while filter has one mismatched entity metric
    // - entity has 2 metrics while filter has 2 matched entity metrics
    // - entity has 2 metrics while filter has 2 entity metrics one of which is matched
    // - entity has 2 metrics while filter has 2 mismatched entity metrics
    // - matched for entity types and ids
    // - mismatched for entity types and ids
    // - matched for entity types and attributes
    // - mismatched for entity types and attributes
    // - matched for entity types and metrics
    // - mismatched for entity types and metrics
    // - matched for entity ids and attributes
    // - mismatched for entity ids and attributes
    // - matched for entity ids and metrics
    // - mismatched for entity ids and metrics
    // - matched for entity attributes and metrics
    // - mismatched for entity attributes and metrics
    // - matched for entity types, ids and attributes
    // - mismatched for entity types, ids and attributes
    // - matched for entity types, ids and metrics
    // - mismatched for entity types, ids and metrics
    // - matched for entity types, attributes and metrics
    // - mismatched for entity types, attributes and metrics
    // - matched for entity ids, attributes and metrics
    // - mismatched for entity ids, attributes and metrics
    // - matched for entity types, ids, attributes and metrics
    // - mismatched for entity types, ids, attributes and metrics
    struct test_case
    {
        metric_entity_prototype *entity_prototype;
        std::unordered_set<std::string> entity_metrics;
        std::string expected_entity_type;
        std::string expected_entity_id;
        metric_entity::attr_map expected_entity_attrs;
        std::unordered_set<std::string> expected_entity_metrics;
        metric_filters::entity_types_type filter_entity_types;
        metric_filters::entity_ids_type filter_entity_ids;
        metric_filters::entity_attrs_type filter_entity_attrs;
        metric_filters::entity_metrics_type filter_entity_metrics;
    } tests[] = {
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_81",
         {},
         kAllServerEntityMetrics,
         {},
         {},
         {},
         {}},
        {&METRIC_ENTITY_my_table,
         kAllTableEntityMetrics,
         "my_table",
         "table_1",
         {{"table", "test_table_1"}},
         kAllTableEntityMetrics,
         {},
         {},
         {},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_1.0",
         {{"table", "test_table_1"}, {"partition", "0"}},
         kAllReplicaEntityMetrics,
         {},
         {},
         {},
         {}},
        {&METRIC_ENTITY_my_replica,
         {},
         "my_replica",
         "replica_1.1",
         {{"table", "test_table_1"}, {"partition", "1"}},
         {},
         {},
         {},
         {},
         {}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_82",
         {},
         kAllServerEntityMetrics,
         {"my_server"},
         {},
         {},
         {}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_83",
         {},
         {},
         {"another_server"},
         {},
         {},
         {}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_84",
         {},
         kAllServerEntityMetrics,
         {"another_server", "my_server"},
         {},
         {},
         {}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_85",
         {},
         {},
         {"another_server", "another_another_server"},
         {},
         {},
         {}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_86",
         {},
         kAllServerEntityMetrics,
         {},
         {"server_86"},
         {},
         {}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_87",
         {},
         {},
         {},
         {"another_server_87"},
         {},
         {}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_88",
         {},
         kAllServerEntityMetrics,
         {},
         {"another_server_88", "server_88"},
         {},
         {}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_89",
         {},
         {},
         {},
         {"another_server_89", "another_another_server_89"},
         {},
         {}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_90",
         {},
         {},
         {},
         {},
         {"attr_name_1", "attr_value_1"},
         {}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_91",
         {},
         {},
         {},
         {},
         {"attr_name_1", "attr_value_1", "attr_name_2", "attr_value_2"},
         {}},
        {&METRIC_ENTITY_my_table,
         kAllTableEntityMetrics,
         "my_table",
         "table_2",
         {{"table", "test_table_2"}},
         kAllTableEntityMetrics,
         {},
         {},
         {"table", "test_table_2"},
         {}},
        {&METRIC_ENTITY_my_table,
         kAllTableEntityMetrics,
         "my_table",
         "table_3",
         {{"table", "test_table_3"}},
         {},
         {},
         {},
         {"another_table", "test_table_3"},
         {}},
        {&METRIC_ENTITY_my_table,
         kAllTableEntityMetrics,
         "my_table",
         "table_4",
         {{"table", "test_table_4"}},
         {},
         {},
         {},
         {"table", "another_test_table_4"},
         {}},
        {&METRIC_ENTITY_my_table,
         kAllTableEntityMetrics,
         "my_table",
         "table_5",
         {{"table", "test_table_5"}},
         {},
         {},
         {},
         {"another_table", "another_test_table_5"},
         {}},
        {&METRIC_ENTITY_my_table,
         kAllTableEntityMetrics,
         "my_table",
         "table_6",
         {{"table", "test_table_6"}},
         kAllTableEntityMetrics,
         {},
         {},
         {"another_table", "another_test_table_6", "table", "test_table_6"},
         {}},
        {&METRIC_ENTITY_my_table,
         kAllTableEntityMetrics,
         "my_table",
         "table_7",
         {{"table", "test_table_7"}},
         {},
         {},
         {},
         {"another_table", "test_table_7", "table", "another_test_table_7"},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_1.2",
         {{"table", "test_table_1"}, {"partition", "2"}},
         kAllReplicaEntityMetrics,
         {},
         {},
         {"table", "test_table_1"},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_1.3",
         {{"table", "test_table_1"}, {"partition", "3"}},
         {},
         {},
         {},
         {"another_partition", "3"},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_1.4",
         {{"table", "test_table_1"}, {"partition", "4"}},
         {},
         {},
         {},
         {"table", "another_test_table_1"},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_1.5",
         {{"table", "test_table_1"}, {"partition", "5"}},
         {},
         {},
         {},
         {"another_table", "another_test_table_1"},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_1.6",
         {{"table", "test_table_1"}, {"partition", "6"}},
         kAllReplicaEntityMetrics,
         {},
         {},
         {"table", "test_table_1", "partition", "6"},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_1.7",
         {{"table", "test_table_1"}, {"partition", "7"}},
         kAllReplicaEntityMetrics,
         {},
         {},
         {"another_table", "another_test_table_1", "partition", "7"},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_2.0",
         {{"table", "test_table_2"}, {"partition", "0"}},
         {},
         {},
         {},
         {"table", "another_test_table_2", "partition", "1"},
         {}},
        {&METRIC_ENTITY_my_server,
         {},
         "my_server",
         "server_92",
         {},
         {},
         {},
         {},
         {},
         {"test_server_gauge_int64"}},
        {&METRIC_ENTITY_my_server,
         {},
         "my_server",
         "server_93",
         {},
         {},
         {},
         {},
         {},
         {"test_server_gauge_int64", "test_server_counter"}},
        {&METRIC_ENTITY_my_server,
         {"test_server_gauge_int64"},
         "my_server",
         "server_94",
         {},
         {"test_server_gauge_int64"},
         {},
         {},
         {},
         {"test_server_gauge_int64"}},
        {&METRIC_ENTITY_my_server,
         {"test_server_gauge_int64"},
         "my_server",
         "server_95",
         {},
         {},
         {},
         {},
         {},
         {"test_server_counter"}},
        {&METRIC_ENTITY_my_server,
         {"test_server_gauge_int64"},
         "my_server",
         "server_96",
         {},
         {"test_server_gauge_int64"},
         {},
         {},
         {},
         {"test_server_gauge_int64", "test_server_counter"}},
        {&METRIC_ENTITY_my_server,
         {"test_server_gauge_int64"},
         "my_server",
         "server_97",
         {},
         {},
         {},
         {},
         {},
         {"test_server_gauge_double", "test_server_counter"}},
        {&METRIC_ENTITY_my_server,
         {"test_server_gauge_int64", "test_server_counter"},
         "my_server",
         "server_98",
         {},
         {"test_server_counter"},
         {},
         {},
         {},
         {"test_server_counter"}},
        {&METRIC_ENTITY_my_server,
         {"test_server_gauge_int64", "test_server_counter"},
         "my_server",
         "server_99",
         {},
         {},
         {},
         {},
         {},
         {"test_server_gauge_double"}},
        {&METRIC_ENTITY_my_server,
         {"test_server_gauge_int64", "test_server_counter"},
         "my_server",
         "server_100",
         {},
         {"test_server_gauge_int64", "test_server_counter"},
         {},
         {},
         {},
         {"test_server_gauge_int64", "test_server_counter"}},
        {&METRIC_ENTITY_my_server,
         {"test_server_gauge_int64", "test_server_counter"},
         "my_server",
         "server_101",
         {},
         {"test_server_gauge_int64"},
         {},
         {},
         {},
         {"test_server_gauge_int64", "test_server_gauge_double"}},
        {&METRIC_ENTITY_my_server,
         {"test_server_gauge_int64", "test_server_counter"},
         "my_server",
         "server_102",
         {},
         {},
         {},
         {},
         {},
         {"test_server_gauge_double", "test_server_concurrent_counter"}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_103",
         {},
         kAllServerEntityMetrics,
         {"another_server", "my_server"},
         {"another_server_103", "server_103"},
         {},
         {}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_104",
         {},
         {},
         {"another_server", "my_server"},
         {"another_server_104"},
         {},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_2.1",
         {{"table", "test_table_2"}, {"partition", "1"}},
         kAllReplicaEntityMetrics,
         {"another_replica", "my_replica"},
         {},
         {"table", "test_table_2", "partition", "1"},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_2.2",
         {{"table", "test_table_2"}, {"partition", "2"}},
         {},
         {"another_replica", "my_replica"},
         {},
         {"table", "another_test_table_2", "another_partition", "2"},
         {}},
        {&METRIC_ENTITY_my_server,
         {"test_server_gauge_int64", "test_server_counter"},
         "my_server",
         "server_105",
         {},
         {"test_server_gauge_int64", "test_server_counter"},
         {"another_server", "my_server"},
         {},
         {},
         {"test_server_gauge_int64", "test_server_counter"}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_106",
         {},
         {"test_server_counter"},
         {"another_server", "my_server"},
         {},
         {},
         {"test_server_gauge_double", "test_server_counter"}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_2.3",
         {{"table", "test_table_2"}, {"partition", "3"}},
         kAllReplicaEntityMetrics,
         {},
         {"another_replica_2.3", "replica_2.3"},
         {"table", "another_test_table_2", "partition", "3"},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_2.4",
         {{"table", "test_table_2"}, {"partition", "4"}},
         {},
         {},
         {"another_replica_2.4", "replica_2.4"},
         {"table", "another_test_table_2", "another_partition", "4"},
         {}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_107",
         {},
         {"test_server_gauge_int64"},
         {},
         {"another_server_107", "server_107"},
         {},
         {"test_server_gauge_int64", "test_server_gauge_double"}},
        {&METRIC_ENTITY_my_server,
         kAllServerEntityMetrics,
         "my_server",
         "server_108",
         {},
         {},
         {},
         {"another_server_108", "server_108"},
         {},
         {"test_server_gauge_double", "test_server_concurrent_counter"}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_2.5",
         {{"table", "test_table_2"}, {"partition", "5"}},
         {"test_replica_gauge_int64"},
         {},
         {},
         {"table", "another_test_table_2", "partition", "5"},
         {"test_replica_gauge_int64", "test_replica_gauge_double"}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_2.6",
         {{"table", "test_table_2"}, {"partition", "6"}},
         {},
         {},
         {},
         {"table", "test_table_2", "partition", "6"},
         {"test_replica_gauge_double", "test_replica_concurrent_counter"}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_2.7",
         {{"table", "test_table_2"}, {"partition", "7"}},
         kAllReplicaEntityMetrics,
         {"another_replica", "my_replica"},
         {"another_replica_2.7", "replica_2.7"},
         {"table", "another_test_table_2", "partition", "7"},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_3.0",
         {{"table", "test_table_3"}, {"partition", "0"}},
         {},
         {"another_replica", "my_replica"},
         {"another_replica_3.0", "replica_3.0"},
         {"table", "another_test_table_3", "another_partition", "0"},
         {}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_3.1",
         {},
         {"test_replica_counter"},
         {"another_replica", "my_replica"},
         {"another_replica_3.1", "replica_3.1"},
         {},
         {"test_replica_gauge_double", "test_replica_counter"}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_3.2",
         {},
         {},
         {"another_replica", "my_replica"},
         {"another_replica_3.2", "replica_3.2"},
         {},
         {"test_replica_gauge_double", "test_replica_concurrent_counter"}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_3.3",
         {{"table", "test_table_3"}, {"partition", "3"}},
         {"test_replica_gauge_int64"},
         {"another_replica", "my_replica"},
         {},
         {"table", "test_table_3", "another_partition", "3"},
         {"test_replica_gauge_int64", "test_replica_gauge_double"}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_3.4",
         {{"table", "test_table_3"}, {"partition", "4"}},
         {},
         {"another_replica", "my_replica"},
         {},
         {"table", "another_test_table_3", "partition", "4"},
         {"test_replica_gauge_double", "test_replica_concurrent_counter"}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_3.5",
         {{"table", "test_table_3"}, {"partition", "5"}},
         {"test_replica_counter"},
         {},
         {"another_replica_3.5", "replica_3.5"},
         {"table", "test_table_3", "another_partition", "5"},
         {"test_replica_gauge_double", "test_replica_counter"}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_3.6",
         {{"table", "test_table_3"}, {"partition", "6"}},
         {},
         {},
         {"another_replica_3.6", "replica_3.6"},
         {"table", "another_test_table_3", "partition", "6"},
         {"test_replica_gauge_double", "test_replica_concurrent_counter"}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_3.7",
         {{"table", "test_table_3"}, {"partition", "7"}},
         {"test_replica_gauge_int64"},
         {"another_replica", "my_replica"},
         {"another_replica_3.7", "replica_3.7"},
         {"table", "test_table_3", "another_partition", "7"},
         {"test_replica_gauge_int64", "test_replica_gauge_double"}},
        {&METRIC_ENTITY_my_replica,
         kAllReplicaEntityMetrics,
         "my_replica",
         "replica_4.0",
         {{"table", "test_table_4"}, {"partition", "0"}},
         {},
         {"another_replica", "my_replica"},
         {"another_replica_4.0", "replica_4.0"},
         {"table", "another_test_table_4", "partition", "0"},
         {"test_replica_gauge_double", "test_replica_concurrent_counter"}},
    };

    for (const auto &test : tests) {
        auto my_entity =
            test.entity_prototype->instantiate(test.expected_entity_id, test.expected_entity_attrs);

        for (const auto &entity_metric : test.entity_metrics) {
            if (entity_metric == "test_server_gauge_int64") {
                auto my_metric = METRIC_test_server_gauge_int64.instantiate(my_entity);
                my_metric->set(5);
            } else if (entity_metric == "test_table_gauge_int64") {
                auto my_metric = METRIC_test_table_gauge_int64.instantiate(my_entity);
                my_metric->set(5);
            } else if (entity_metric == "test_replica_gauge_int64") {
                auto my_metric = METRIC_test_replica_gauge_int64.instantiate(my_entity);
                my_metric->set(5);
            } else if (entity_metric == "test_server_counter") {
                auto my_metric = METRIC_test_server_counter.instantiate(my_entity);
                my_metric->increment();
            } else if (entity_metric == "test_table_counter") {
                auto my_metric = METRIC_test_table_counter.instantiate(my_entity);
                my_metric->increment();
            } else if (entity_metric == "test_replica_counter") {
                auto my_metric = METRIC_test_replica_counter.instantiate(my_entity);
                my_metric->increment();
            }
        }

        metric_filters filters;
        filters.entity_types = test.filter_entity_types;
        filters.entity_ids = test.filter_entity_ids;
        filters.entity_attrs = test.filter_entity_attrs;
        filters.entity_metrics = test.filter_entity_metrics;

        auto json_string = take_snapshot_and_get_json_string(my_entity.get(), filters);
        check_entity_from_json_string(my_entity,
                                      json_string,
                                      test.expected_entity_type,
                                      test.expected_entity_id,
                                      test.expected_entity_attrs,
                                      test.expected_entity_metrics);
    }
}

void check_entity_ids_from_json_string(const std::string &json_string,
                                       const std::unordered_set<std::string> &expected_entity_ids)
{
    // Even if there is not any entity selected, `json_string` should be "[]".
    ASSERT_FALSE(json_string.empty());

    rapidjson::Document doc;
    rapidjson::ParseResult result = doc.Parse(json_string.c_str());
    ASSERT_FALSE(result.IsError());

    // Actual entity ids parsed from json string.
    std::unordered_set<std::string> actual_entity_ids;

    // The json format for entities should be an array.
    ASSERT_TRUE(doc.IsArray());
    for (const auto &entity : doc.GetArray()) {
        // The json format for each entity should be an object.
        ASSERT_TRUE(entity.IsObject());

        for (const auto &elem : entity.GetObject()) {
            // Each name must be a string.
            ASSERT_TRUE(elem.name.IsString());

            if (kMetricEntityTypeField == elem.name.GetString()) {
                ASSERT_STREQ("my_server", elem.value.GetString());
            } else if (kMetricEntityIdField == elem.name.GetString()) {
                actual_entity_ids.emplace(elem.value.GetString());
            } else if (kMetricEntityAttrsField == elem.name.GetString()) {
                ASSERT_TRUE(elem.value.ObjectEmpty());
            } else if (kMetricEntityMetricsField == elem.name.GetString()) {
                ASSERT_TRUE(elem.value.IsArray());

                std::unordered_set<std::string> actual_entity_metrics;
                for (const auto &m : elem.value.GetArray()) {
                    ASSERT_TRUE(m.IsObject());

                    for (const auto &field : m.GetObject()) {
                        // Each name must be a string.
                        ASSERT_TRUE(field.name.IsString());
                        if (kMetricNameField == field.name.GetString()) {
                            ASSERT_TRUE(field.value.IsString());
                            actual_entity_metrics.emplace(field.value.GetString());
                        }
                    }
                }

                static const std::unordered_set<std::string> kExpectedEntityMetrics = {
                    "test_server_gauge_int64", "test_server_counter"};
                ASSERT_EQ(kExpectedEntityMetrics, actual_entity_metrics);
            } else {
                ASSERT_TRUE(false) << "invalid field name: " << elem.name.GetString();
            }
        }
    }

    ASSERT_EQ(expected_entity_ids, actual_entity_ids);
}

void check_registry_json_string(const std::unordered_set<std::string> &entity_ids,
                                const metric_filters::entity_ids_type &filter_entity_ids,
                                const std::unordered_set<std::string> &expected_entity_ids)
{
    for (const auto &id : entity_ids) {
        auto my_entity = METRIC_ENTITY_my_server.instantiate(id);

        auto my_gauge_int64 = METRIC_test_server_gauge_int64.instantiate(my_entity);
        my_gauge_int64->set(5);

        auto my_counter = METRIC_test_server_counter.instantiate(my_entity);
        my_counter->increment();
    }

    metric_filters filters;
    filters.entity_ids = filter_entity_ids;

    auto &registery = metric_registry::instance();
    auto json_string = take_snapshot_and_get_json_string(&registery, filters);
    check_entity_ids_from_json_string(json_string, expected_entity_ids);
}

TEST(metrics_test, take_snapshot_registry)
{
    // Test cases:
    // - filter an entity that does not exist in registery
    // - filter 2 entities both of which do not exist in registery
    // - filter an entity that exists in registery
    // - filter 2 entities one of which does not exist in registery
    // - filter 2 entities both of which exist in registery
    // - filter 3 entities one of which does not exist in registery
    struct test_case
    {
        std::unordered_set<std::string> entity_ids;
        metric_filters::entity_ids_type filter_entity_ids;
        std::unordered_set<std::string> expected_entity_ids;
    } tests[] = {
        {{}, {"server_109"}, {}},
        {{}, {"server_109", "server_110"}, {}},
        {{"server_109"}, {"server_109"}, {"server_109"}},
        {{"server_110"}, {"server_110", "server_111"}, {"server_110"}},
        {{"server_111", "server_112"}, {"server_111", "server_112"}, {"server_111", "server_112"}},
        {{"server_113", "server_114"},
         {"server_113", "server_114", "server_115"},
         {"server_113", "server_114"}},
    };

    for (const auto &test : tests) {
        check_registry_json_string(
            test.entity_ids, test.filter_entity_ids, test.expected_entity_ids);
    }
}

struct entity_properties
{
    std::string type;
    metric_entity::attr_map attrs;
    std::unordered_set<std::string> metrics;
};

bool operator==(const entity_properties &lhs, const entity_properties &rhs)
{
    if (lhs.type != rhs.type) {
        return false;
    }

    if (lhs.attrs != rhs.attrs) {
        return false;
    }

    return lhs.metrics == rhs.metrics;
}

using entity_container = std::unordered_map<std::string, entity_properties>;

void check_entities_from_json_string(const std::string &json_string,
                                     const http_status_code expected_status_code,
                                     const entity_container &expected_entities,
                                     const std::unordered_set<std::string> &expected_metric_fields)
{
    // Should not be empty for both success and error response.
    ASSERT_FALSE(json_string.empty());

    rapidjson::Document doc;
    rapidjson::ParseResult result = doc.Parse(json_string.c_str());
    ASSERT_FALSE(result.IsError());

    if (expected_status_code != http_status_code::ok) {
        // Error response is an object in essence.
        ASSERT_TRUE(doc.IsObject());

        for (const auto &elem : doc.GetObject()) {
            // Each name must be a string.
            ASSERT_TRUE(elem.name.IsString());

            // There is only one field for error response.
            ASSERT_STREQ("error_message", elem.name.GetString());

            ASSERT_TRUE(elem.value.IsString());
            std::cout << "error_message: " << elem.value.GetString() << std::endl;
        }
        return;
    }

    // Actual entities parsed from json string.
    entity_container actual_entities;

    // The json format for entities should be an array.
    ASSERT_TRUE(doc.IsArray());
    for (const auto &entity : doc.GetArray()) {
        // The json format for each entity should be an object.
        ASSERT_TRUE(entity.IsObject());

        // Actual properties parsed from json string for each entity.
        std::string id;
        entity_properties actual_entity;

        // Actual fields parsed from json string for each entity.
        std::unordered_set<std::string> actual_entity_fields;

        for (const auto &elem : entity.GetObject()) {
            // Each name must be a string.
            ASSERT_TRUE(elem.name.IsString());

            if (kMetricEntityTypeField == elem.name.GetString()) {
                ASSERT_TRUE(elem.value.IsString());
                actual_entity.type = elem.value.GetString();
            } else if (kMetricEntityIdField == elem.name.GetString()) {
                ASSERT_TRUE(elem.value.IsString());
                id = elem.value.GetString();
            } else if (kMetricEntityAttrsField == elem.name.GetString()) {
                ASSERT_TRUE(elem.value.IsObject());

                for (const auto &attr : elem.value.GetObject()) {
                    // Each name must be a string.
                    ASSERT_TRUE(attr.name.IsString());
                    ASSERT_TRUE(attr.value.IsString());
                    actual_entity.attrs.emplace(attr.name.GetString(), attr.value.GetString());
                }
            } else if (kMetricEntityMetricsField == elem.name.GetString()) {
                ASSERT_TRUE(elem.value.IsArray());

                for (const auto &m : elem.value.GetArray()) {
                    ASSERT_TRUE(m.IsObject());

                    // Actual fields for each metric.
                    std::unordered_set<std::string> actual_metric_fields;

                    bool has_metric_name_field = false;
                    for (const auto &field : m.GetObject()) {
                        // Each name must be a string.
                        ASSERT_TRUE(field.name.IsString());
                        if (kMetricNameField == field.name.GetString()) {
                            ASSERT_TRUE(field.value.IsString());
                            actual_entity.metrics.emplace(field.value.GetString());
                            has_metric_name_field = true;
                        }
                        actual_metric_fields.emplace(field.name.GetString());
                    }
                    ASSERT_TRUE(has_metric_name_field)
                        << "metric name must be included in the fields";

                    // Check if all parsed fields for each metric are expected.
                    ASSERT_EQ(expected_metric_fields, actual_metric_fields);
                }
            } else {
                ASSERT_TRUE(false) << "invalid field name: " << elem.name.GetString();
            }

            actual_entity_fields.emplace(elem.name.GetString());
        }

        // Check if all parsed fields for each entity are expected.
        ASSERT_EQ(kAllMetricEntityFields, actual_entity_fields);

        actual_entities.emplace(id, actual_entity);
    }

    // Check if the contents of all entities are identical with the expected.
    ASSERT_EQ(expected_entities, actual_entities);
}

void test_get_metrics_handler(const http_request &req, http_response &resp)
{
    metric_registry::instance()._http_service.get_metrics_handler(req, resp);
}

void test_http_get_metrics(const std::string &request_string,
                           const http_status_code expected_status_code,
                           const entity_container &expected_entities,
                           const std::unordered_set<std::string> &expected_metric_fields)
{
    std::cout << "request_string: " << request_string << std::endl;

    message_reader reader(64);
    char *buf = reader.read_buffer_ptr(request_string.size());
    std::memcpy(buf, request_string.data(), request_string.size());
    reader.mark_read(request_string.size());

    http_message_parser parser;
    int read_next = 0;
    message_ptr msg(parser.get_message_on_receive(&reader, read_next));
    ASSERT_NE(msg, nullptr);

    const auto &req_res = http_request::parse(msg.get());
    ASSERT_TRUE(req_res.is_ok());

    const auto &req = req_res.get_value();
    std::cout << "method: " << req.method << std::endl;

    http_response resp;
    test_get_metrics_handler(req, resp);

    ASSERT_EQ(expected_status_code, resp.status_code);
    check_entities_from_json_string(
        resp.body, expected_status_code, expected_entities, expected_metric_fields);
}

TEST(metrics_test, http_get_metrics)
{
    struct loaded_test_entity
    {
        metric_entity_prototype *prototype;
        std::string id;
        metric_entity::attr_map attrs;
    } test_entities[] = {
        {&METRIC_ENTITY_my_replica, "replica_5.0", {{"table", "test_app_5"}, {"partition", "5.0"}}},
        {&METRIC_ENTITY_my_replica, "replica_5.1", {{"table", "test_app_5"}, {"partition", "5.1"}}},
        {&METRIC_ENTITY_my_app, "app_5", {{"table", "test_app_5"}}},
        {&METRIC_ENTITY_my_app, "app_6", {{"table", "test_app_6"}}},
        {&METRIC_ENTITY_my_server, "server_116", {}},
    };

    for (const auto &entity : test_entities) {
        auto my_entity = entity.prototype->instantiate(entity.id, entity.attrs);

        if (utils::equals(entity.prototype->name(), "my_server")) {
            static const std::set<kth_percentile_type> kPercentileTypes = {
                kth_percentile_type::P95, kth_percentile_type::P99};
            auto my_metric = METRIC_test_server_percentile_int64.instantiate(
                my_entity, 50UL, kPercentileTypes, 4096UL);
            my_metric->set(5);
            continue;
        }

        gauge_ptr<int64_t> my_gauge_int64;
        counter_ptr<> my_counter;
        if (utils::equals(entity.prototype->name(), "my_replica")) {
            my_gauge_int64 = METRIC_test_replica_gauge_int64.instantiate(my_entity);
            my_counter = METRIC_test_replica_counter.instantiate(my_entity);
        } else if (utils::equals(entity.prototype->name(), "my_app")) {
            my_gauge_int64 = METRIC_test_app_gauge_int64.instantiate(my_entity);
            my_counter = METRIC_test_app_counter.instantiate(my_entity);
        } else {
            ASSERT_TRUE(false);
        }

        // Set metrics with arbitrary values.
        my_gauge_int64->set(5);
        my_counter->increment();
    }

// Do not use leading '#' for `fields` to replace with the literal text, since clang-format
// will keep a space before and after '=' in `fields`. Just use double quotes "" instead.
#define REQUEST_STRING(method, fields) (#method " /metrics?" fields " HTTP/1.1\r\n\r\n")

    static const metric_filters::metric_fields_type kBriefSingleValueMetricFields = {
        kMetricNameField, kMetricSingleValueField};

    auto percentile_metric_fields = kAllPrototypeMetricFields;
    percentile_metric_fields.emplace("p95");
    percentile_metric_fields.emplace("p99");

    // Test cases:
    // - get all metrics that belong to an entity type of "my_app"
    // - request by POST method
    // - request with an unknown field name in query string
    // - request for an entity types which does not exist
    // - request for 2 entity types one of which does not exist
    // - request for 2 entity types both of which exist
    // - request for 2 entity types both of which do not exist
    // - request for an entity id which exists
    // - request for an entity id which does not exist
    // - request for 2 entity ids both of which exist
    // - request for 2 entity ids one of which does not exist
    // - request for 2 entity ids both of which do not exist
    // - request for 1 pair of entity attribute that exists
    // - request for 1 pair of entity attribute that does not exist
    // - 2 pairs of entity attributes both of which exist have the different keys
    // - 2 pairs of entity attributes both of which exist have the same key
    // - 2 pair of entity attribute one of which does not exist have the different keys
    // - 2 pair of entity attribute one of which does not exist have the same keys
    // - the number of arguments for entity attributes in query string is odd(1)
    // - the number of arguments for entity attributes in query string is odd(3)
    // - request for a metric which exists
    // - request for a metric which does not exist
    // - request for 2 metrics ids both of which exist
    // - request for 2 metrics one of which does not exist
    // - request for 2 metrics both of which do not exist
    // - request for one metric field
    // - request for 2 metric fields
    // - filter with types and ids
    // - filter with types, ids and attributes
    // - filter with ids and attributes
    // - filter with ids and metrics
    // - filter with attributes and metrics
    // - filter with ids, attributes and metrics
    // - filter with types, ids, attributes and metrics while request for specified fields
    // - request gauge and counter with specified fields while detail=false
    // - request gauge and counter with specified fields while detail=true
    // - request gauge and counter while detail=false
    // - request gauge and counter while detail=true
    // - request percentile with specified fields for default detail
    // - request percentile with specified fields while detail=false
    // - request percentile with specified fields while detail=true
    // - request percentile for default detail
    // - request percentile while detail=false
    // - request percentile while detail=true
    struct test_case
    {
        std::string request_string;
        http_status_code expected_status_code;
        std::unordered_map<std::string, std::unordered_set<std::string>> expected_entity_metrics;
        std::unordered_set<std::string> expected_metric_fields;
    } tests[] = {
        {REQUEST_STRING(GET, "types=my_app"),
         http_status_code::ok,
         {{"app_5", {"test_app_gauge_int64", "test_app_counter"}},
          {"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(POST, "types=my_app"), http_status_code::bad_request, {}, {}},
        {REQUEST_STRING(POST, "invalid_field=unknown_value"),
         http_status_code::bad_request,
         {},
         {}},
        {REQUEST_STRING(GET, "types=unknown_type"), http_status_code::ok, {}, {}},
        {REQUEST_STRING(GET, "types=unknown_type,my_app"),
         http_status_code::ok,
         {{"app_5", {"test_app_gauge_int64", "test_app_counter"}},
          {"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "types=my_app,my_replica&attributes=table,test_app_5"),
         http_status_code::ok,
         {{"replica_5.0", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"replica_5.1", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"app_5", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "types=unknown_type_1,unknown_type_2"), http_status_code::ok, {}, {}},
        {REQUEST_STRING(GET, "ids=replica_5.1"),
         http_status_code::ok,
         {{"replica_5.1", {"test_replica_gauge_int64", "test_replica_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "ids=another_replica_5.1"), http_status_code::ok, {}, {}},
        {REQUEST_STRING(GET, "ids=replica_5.1,app_6"),
         http_status_code::ok,
         {{"replica_5.1", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "ids=another_replica_5.1,app_6"),
         http_status_code::ok,
         {{"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "ids=another_replica_5.1,another_app_6"),
         http_status_code::ok,
         {},
         {}},
        {REQUEST_STRING(GET, "attributes=table,test_app_5"),
         http_status_code::ok,
         {{"replica_5.0", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"replica_5.1", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"app_5", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "attributes=table,another_test_app_5"), http_status_code::ok, {}, {}},
        {REQUEST_STRING(GET, "attributes=table,test_app_5,partition,5.1"),
         http_status_code::ok,
         {{"replica_5.0", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"replica_5.1", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"app_5", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "attributes=table,test_app_5,table,test_app_6"),
         http_status_code::ok,
         {{"replica_5.0", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"replica_5.1", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"app_5", {"test_app_gauge_int64", "test_app_counter"}},
          {"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "attributes=table,test_app_5,partition,another_5.1"),
         http_status_code::ok,
         {{"replica_5.0", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"replica_5.1", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"app_5", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "attributes=table,test_app_5,table,another_test_app_6"),
         http_status_code::ok,
         {{"replica_5.0", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"replica_5.1", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"app_5", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "attributes=table"), http_status_code::bad_request, {}, {}},
        {REQUEST_STRING(GET, "attributes=table,test_app_5,partition"),
         http_status_code::bad_request,
         {},
         {}},
        {REQUEST_STRING(GET, "metrics=test_app_gauge_int64"),
         http_status_code::ok,
         {{"app_5", {"test_app_gauge_int64"}}, {"app_6", {"test_app_gauge_int64"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "metrics=another_test_app_gauge_int64"), http_status_code::ok, {}, {}},
        {REQUEST_STRING(GET, "metrics=test_app_gauge_int64,test_app_counter"),
         http_status_code::ok,
         {{"app_5", {"test_app_gauge_int64", "test_app_counter"}},
          {"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "metrics=test_app_gauge_int64,another_test_app_gauge_int64"),
         http_status_code::ok,
         {{"app_5", {"test_app_gauge_int64"}}, {"app_6", {"test_app_gauge_int64"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "metrics=another_test_app_gauge_int64,another_test_app_counter"),
         http_status_code::ok,
         {},
         {}},
        {REQUEST_STRING(GET, "types=my_app&with_metric_fields=name"),
         http_status_code::ok,
         {{"app_5", {"test_app_gauge_int64", "test_app_counter"}},
          {"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         {kMetricNameField}},
        {REQUEST_STRING(GET, "types=my_app&with_metric_fields=name,value"),
         http_status_code::ok,
         {{"app_5", {"test_app_gauge_int64", "test_app_counter"}},
          {"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         {kMetricNameField, kMetricSingleValueField}},
        {REQUEST_STRING(GET, "types=my_app,my_replica&ids=replica_5.1,app_6"),
         http_status_code::ok,
         {{"replica_5.1", {"test_replica_gauge_int64", "test_replica_counter"}},
          {"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(
             GET, "types=my_app,my_replica&ids=replica_5.1,app_6&attributes=table,test_app_5"),
         http_status_code::ok,
         {{"replica_5.1", {"test_replica_gauge_int64", "test_replica_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "ids=replica_5.1,app_6&attributes=table,test_app_6"),
         http_status_code::ok,
         {{"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET,
                        "ids=replica_5.1,app_6&metrics=test_replica_gauge_int64,test_app_counter"),
         http_status_code::ok,
         {{"replica_5.1", {"test_replica_gauge_int64"}}, {"app_6", {"test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(
             GET, "attributes=table,test_app_5&metrics=test_replica_counter,test_app_gauge_int64"),
         http_status_code::ok,
         {{"replica_5.0", {"test_replica_counter"}},
          {"replica_5.1", {"test_replica_counter"}},
          {"app_5", {"test_app_gauge_int64"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET,
                        "ids=replica_5.1,app_5&attributes=table,test_app_5&metrics=test_"
                        "replica_counter,test_app_counter"),
         http_status_code::ok,
         {{"replica_5.1", {"test_replica_counter"}}, {"app_5", {"test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(
             GET,
             "types=my_app,ids=replica_5.1,app_6&attributes=table,test_app_6&"
             "metrics=test_replica_counter,test_app_counter&with_metric_fields=name,value"),
         http_status_code::ok,
         {{"app_6", {"test_app_counter"}}},
         {kMetricNameField, kMetricSingleValueField}},
        {REQUEST_STRING(GET, "types=my_app&with_metric_fields=name,desc&detail=false"),
         http_status_code::ok,
         {{"app_5", {"test_app_gauge_int64", "test_app_counter"}},
          {"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         {kMetricNameField, kMetricDescField}},
        {REQUEST_STRING(GET, "types=my_app&with_metric_fields=name,desc&detail=true"),
         http_status_code::ok,
         {{"app_5", {"test_app_gauge_int64", "test_app_counter"}},
          {"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         {kMetricNameField, kMetricDescField}},
        {REQUEST_STRING(GET, "types=my_app&detail=false"),
         http_status_code::ok,
         {{"app_5", {"test_app_gauge_int64", "test_app_counter"}},
          {"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         kBriefSingleValueMetricFields},
        {REQUEST_STRING(GET, "types=my_app&detail=true"),
         http_status_code::ok,
         {{"app_5", {"test_app_gauge_int64", "test_app_counter"}},
          {"app_6", {"test_app_gauge_int64", "test_app_counter"}}},
         kAllSingleValueMetricFields},
        {REQUEST_STRING(GET, "ids=server_116&with_metric_fields=name,desc"),
         http_status_code::ok,
         {{"server_116", {"test_server_percentile_int64"}}},
         {kMetricNameField, kMetricDescField}},
        {REQUEST_STRING(GET, "ids=server_116&with_metric_fields=name,desc&detail=false"),
         http_status_code::ok,
         {{"server_116", {"test_server_percentile_int64"}}},
         {kMetricNameField, kMetricDescField}},
        {REQUEST_STRING(GET, "ids=server_116&with_metric_fields=name,desc&detail=true"),
         http_status_code::ok,
         {{"server_116", {"test_server_percentile_int64"}}},
         {kMetricNameField, kMetricDescField}},
        {REQUEST_STRING(GET, "ids=server_116"),
         http_status_code::ok,
         {{"server_116", {"test_server_percentile_int64"}}},
         {kMetricNameField, "p95", "p99"}},
        {REQUEST_STRING(GET, "ids=server_116&detail=false"),
         http_status_code::ok,
         {{"server_116", {"test_server_percentile_int64"}}},
         {kMetricNameField, "p95", "p99"}},
        {REQUEST_STRING(GET, "ids=server_116&detail=true"),
         http_status_code::ok,
         {{"server_116", {"test_server_percentile_int64"}}},
         percentile_metric_fields},
    };

#undef REQUEST_STRING

    const auto &entities = metric_registry::instance().entities();
    for (const auto &test : tests) {
        entity_container expected_entities;
        for (const auto &entity_pair : test.expected_entity_metrics) {
            const auto &iter = entities.find(entity_pair.first);
            ASSERT_NE(entities.end(), iter);

            const auto &entity = iter->second;
            expected_entities.emplace(entity->id(),
                                      entity_properties{entity->prototype()->name(),
                                                        entity->attributes(),
                                                        entity_pair.second});
        }

        test_http_get_metrics(test.request_string,
                              test.expected_status_code,
                              expected_entities,
                              test.expected_metric_fields);
    }
}

using surviving_metrics_case = std::tuple<std::string, bool, bool, bool, bool>;

class MetricsRetirementTest : public testing::TestWithParam<surviving_metrics_case>
{
public:
    // For higher version of googletest, use `static void SetUpTestSuite()` instead.
    static void SetUpTestCase()
    {
        // Restart the timer of registry with shorter interval to reduce the test time.
        _reserved_entity_retirement_delay_ms = FLAGS_entity_retirement_delay_ms;
        restart_metric_registry_timer(kEntityRetirementDelayMsForTest);
    }

    // For higher version of googletest, use `static void TearDownTestSuite()` instead.
    static void TearDownTestCase()
    {
        // Recover the timer of registry with the original interval.
        restart_metric_registry_timer(_reserved_entity_retirement_delay_ms);
    }

    static const uint64_t kEntityRetirementDelayMsForTest;

private:
    static void restart_metric_registry_timer(uint64_t interval_ms)
    {
        metric_registry::instance().stop_timer();
        FLAGS_entity_retirement_delay_ms = interval_ms;
        metric_registry::instance().start_timer();

        std::cout << "restart the timer of metric registry at interval " << interval_ms << " ms."
                  << std::endl;
    }

    static uint64_t _reserved_entity_retirement_delay_ms;
};

const uint64_t MetricsRetirementTest::kEntityRetirementDelayMsForTest = 100;
uint64_t MetricsRetirementTest::_reserved_entity_retirement_delay_ms;

// This class helps to test retirement of metrics and entities, by creating temporary
// variables or reference them as members of this class to control their lifetime.
class scoped_entity
{
public:
    // Use the raw pointer to hold metric without any reference which may affect the test results.
    using surviving_metric_map = std::unordered_map<const metric_prototype *, const metric *>;

    scoped_entity(const std::string &entity_id,
                  bool is_entity_surviving,
                  bool is_gauge_surviving,
                  bool is_counter_surviving,
                  bool is_percentile_surviving);

    // After a long enough time, check if temporary entity is retired with its own metrics while
    // long-life one still survive.
    void test_survival_after_retirement() const;

private:
    template <typename MetricPrototype, typename MetricPtr>
    void instantiate_metric(const metric_entity_ptr &my_entity,
                            bool is_surviving,
                            const MetricPrototype &prototype,
                            MetricPtr &m)
    {
        // Create a temporary variable for the metric.
        auto temp_m = prototype.instantiate(my_entity);
        _expected_all_metrics.emplace(&prototype, temp_m.get());

        if (!is_surviving) {
            return;
        }

        // Extend the lifetime of the metric since it's marked as "surviving".
        m = temp_m;
    }

    surviving_metric_map get_actual_surviving_metrics(const metric_entity_ptr &my_entity) const;

    // Check if the entity still survive with its own metrics no matter whether they are temporary
    // or long-life.
    void test_survival_immediately_after_initialization() const;

    std::string _my_entity_id;
    metric_entity *_expected_my_entity_raw_ptr;
    metric_entity_ptr _my_entity;

    gauge_ptr<int64_t> _my_gauge_int64;
    counter_ptr<> _my_counter;
    percentile_ptr<int64_t> _my_percentile_int64;

    surviving_metric_map _expected_all_metrics;
    surviving_metric_map _expected_surviving_metrics;
};

scoped_entity::scoped_entity(const std::string &entity_id,
                             bool is_entity_surviving,
                             bool is_gauge_surviving,
                             bool is_counter_surviving,
                             bool is_percentile_surviving)
    : _my_entity_id(entity_id)
{
    // Create a temporary variabl for the entity.
    auto my_entity = METRIC_ENTITY_my_server.instantiate(entity_id);
    _expected_my_entity_raw_ptr = my_entity.get();

    // Create temporary or long-life variables for metrics, depending on what is_*_surviving is.
    instantiate_metric(
        my_entity, is_gauge_surviving, METRIC_test_server_gauge_int64, _my_gauge_int64);
    instantiate_metric(my_entity, is_counter_surviving, METRIC_test_server_counter, _my_counter);
    instantiate_metric(my_entity,
                       is_percentile_surviving,
                       METRIC_test_server_percentile_int64,
                       _my_percentile_int64);

    if (is_entity_surviving) {
        // Extend the lifetime of the entity since it's marked as "surviving".
        _my_entity = my_entity;
        _expected_surviving_metrics = _expected_all_metrics;
    }

    test_survival_immediately_after_initialization();
}

scoped_entity::surviving_metric_map
scoped_entity::get_actual_surviving_metrics(const metric_entity_ptr &my_entity) const
{
    surviving_metric_map actual_surviving_metrics;

    utils::auto_read_lock l(my_entity->_lock);

    // Use internal member directly instead of calling metrics(). We don't want to have
    // any reference which may affect the test results.
    for (const auto &m : my_entity->_metrics) {
        actual_surviving_metrics.emplace(m.first, m.second.get());
    }

    return actual_surviving_metrics;
}

void scoped_entity::test_survival_immediately_after_initialization() const
{
    utils::auto_read_lock l(metric_registry::instance()._lock);

    // Use internal member directly instead of calling entities(). We don't want to have
    // any reference which may affect the test results.
    const auto &entities = metric_registry::instance()._entities;
    const auto &iter = entities.find(_my_entity_id);
    ASSERT_NE(entities.end(), iter);
    ASSERT_EQ(_expected_my_entity_raw_ptr, iter->second.get());

    const auto &actual_surviving_metrics = get_actual_surviving_metrics(iter->second);
    ASSERT_EQ(_expected_all_metrics, actual_surviving_metrics);
}

void scoped_entity::test_survival_after_retirement() const
{
    std::this_thread::sleep_for(
        std::chrono::milliseconds(MetricsRetirementTest::kEntityRetirementDelayMsForTest * 2));

    utils::auto_read_lock l(metric_registry::instance()._lock);

    // Use internal member directly instead of calling entities(). We don't want to have
    // any reference which may affect the test results.
    const auto &entities = metric_registry::instance()._entities;
    const auto &iter = entities.find(_my_entity_id);
    if (_my_entity == nullptr) {
        // The entity has been retired.
        ASSERT_EQ(entities.end(), iter);
        ASSERT_TRUE(_expected_surviving_metrics.empty());
        return;
    }

    ASSERT_NE(entities.end(), iter);
    ASSERT_EQ(_expected_my_entity_raw_ptr, iter->second.get());

    const auto &actual_surviving_metrics = get_actual_surviving_metrics(iter->second);
    ASSERT_EQ(_expected_surviving_metrics, actual_surviving_metrics);
}

TEST_P(MetricsRetirementTest, RetireOldMetrics)
{
    std::string entity_id;
    bool is_entity_surviving;
    bool is_gauge_surviving;
    bool is_counter_surviving;
    bool is_percentile_surviving;
    std::tie(entity_id,
             is_entity_surviving,
             is_gauge_surviving,
             is_counter_surviving,
             is_percentile_surviving) = GetParam();

    scoped_entity entity(entity_id,
                         is_entity_surviving,
                         is_gauge_surviving,
                         is_counter_surviving,
                         is_percentile_surviving);
    entity.test_survival_after_retirement();
}

const std::vector<surviving_metrics_case> metrics_retirement_tests = {
    {"server_117", true, true, true, true},
    {"server_118", true, true, true, false},
    {"server_119", true, true, false, false},
    {"server_120", true, false, false, false},
    {"server_121", false, false, false, false},
};

INSTANTIATE_TEST_CASE_P(MetricsTest,
                        MetricsRetirementTest,
                        testing::ValuesIn(metrics_retirement_tests));

} // namespace dsn
