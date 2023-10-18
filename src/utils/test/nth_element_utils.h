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
#include <cstdint>
#include <memory>
#include <random>
#include <type_traits>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "utils/api_utilities.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/process_utils.h"
#include "utils/rand.h"

#include "perf_counter/perf_counter_atomic.h"

namespace dsn {

// The generator is used to produce the test cases randomly for unit tests and benchmarks
// of nth elements.
template <typename T,
          typename Rand,
          typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
class nth_element_case_generator
{
public:
    using value_type = T;
    using container_type = typename std::vector<value_type>;
    using size_type = typename container_type::size_type;
    using nth_container_type = typename std::vector<size_type>;

    nth_element_case_generator(size_type array_size,
                               value_type initial_value,
                               uint64_t range_size,
                               const nth_container_type &nths)
        : _array_size(array_size),
          _initial_value(initial_value),
          _range_size(range_size),
          _nths(nths),
          _rand(Rand())
    {
        CHECK(std::is_sorted(_nths.begin(), _nths.end()),
              "nth indexes({}) is not sorted",
              fmt::join(_nths, " "));

        for (const auto &nth : _nths) {
            CHECK(nth >= 0 && nth < _array_size, "nth should be in the range [0, {})", _array_size);
        }
    }

    ~nth_element_case_generator() = default;

    // Generate an out-of-order `array` sized `_array_size`, and put nth elements of sorted
    // `array` to `elements` in the order of `_nths` which must be sorted.
    //
    // The process has 2 stages:
    // (1) Generate a sorted `array` from _initial_value. Always generate next element by current
    // element plus _rand(_range_size). Once the index of an element belongs to nth indexes, it
    // will be appended to `elements`.
    // (2) After the sorted `array` is generated, it will be shuffled to be out-of-order.
    void operator()(container_type &array, container_type &elements)
    {
        array.clear();
        elements.clear();

        auto value = _initial_value;
        for (size_type i = 0, j = 0; i < _array_size; ++i) {
            array.push_back(value);
            for (; j < _nths.size() && _nths[j] == i; ++j) {
                elements.push_back(value);
            }

            auto delta = _rand(_range_size);
            value += delta;
        }

        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(array.begin(), array.end(), g);
    }

private:
    const size_type _array_size;
    const value_type _initial_value;
    const uint64_t _range_size;
    const nth_container_type _nths;
    const Rand _rand;

    DISALLOW_COPY_AND_ASSIGN(nth_element_case_generator);
};

template <typename T, typename = typename std::enable_if<std::is_integral<T>::value>::type>
class integral_rand_generator
{
public:
    T operator()(const uint64_t &upper) const { return static_cast<T>(rand::next_u64(upper)); }
};

template <typename T, typename = typename std::enable_if<std::is_integral<T>::value>::type>
using integral_nth_element_case_generator =
    nth_element_case_generator<T, integral_rand_generator<T>>;

template <typename T, typename = typename std::enable_if<std::is_floating_point<T>::value>::type>
class floating_rand_generator
{
public:
    T operator()(const uint64_t &upper) const
    {
        return static_cast<T>(rand::next_u64(upper)) +
               static_cast<T>(rand::next_u64(upper)) / static_cast<T>(upper);
    }
};

template <typename T, typename = typename std::enable_if<std::is_floating_point<T>::value>::type>
using floating_nth_element_case_generator =
    nth_element_case_generator<T, floating_rand_generator<T>>;

// Finder class based on perf_counter in comparison with other finders for multiple nth elements.
class perf_counter_nth_element_finder
{
public:
    using container_type = typename std::vector<int64_t>;
    using size_type = typename container_type::size_type;

    perf_counter_nth_element_finder()
        : _perf_counter("benchmark",
                        "perf_counter_number_percentile_atomic",
                        "nth_element",
                        COUNTER_TYPE_NUMBER_PERCENTILES,
                        "nth_element implementation by perf_counter_number_percentile_atomic",
                        false),
          _elements(COUNTER_PERCENTILE_COUNT, int64_t())
    {
    }

    void load_data(const container_type &array)
    {
        _perf_counter._tail.store(0, std::memory_order_relaxed);
        for (const auto &e : array) {
            _perf_counter.set(e);
        }
    }

    void operator()()
    {
        _perf_counter.calc(
            std::make_shared<dsn::perf_counter_number_percentile_atomic::compute_context>());
        std::copy(_perf_counter._results,
                  _perf_counter._results + COUNTER_PERCENTILE_COUNT,
                  _elements.begin());
    }

    const container_type &elements() const { return _elements; }

private:
    dsn::perf_counter_number_percentile_atomic _perf_counter;
    container_type _elements;

    DISALLOW_COPY_AND_ASSIGN(perf_counter_nth_element_finder);
};

} // namespace dsn
