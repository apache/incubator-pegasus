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

#include <memory>
#include <set>
#include <type_traits>
#include <vector>

#include "utils/api_utilities.h"
#include "utils/metrics.h"
#include "utils/fmt_logging.h"

#include "nth_element_utils.h"

namespace dsn {

// The generator is used to produce the test cases randomly for unit tests and benchmarks of
// percentile. This is implemented by converting kth percentiles to nth indexes, and calling
// nth_element_case_generator to generate data and nth elements.
template <typename NthElementCaseGenerator,
          typename = typename std::enable_if<
              std::is_arithmetic<typename NthElementCaseGenerator::value_type>::value>::type>
class percentile_case_generator
{
public:
    using value_type = typename NthElementCaseGenerator::value_type;
    using container_type = typename NthElementCaseGenerator::container_type;
    using size_type = typename NthElementCaseGenerator::size_type;
    using nth_container_type = typename NthElementCaseGenerator::nth_container_type;

    percentile_case_generator(size_type data_size,
                              value_type initial_value,
                              uint64_t range_size,
                              const std::set<kth_percentile_type> &kth_percentiles)
        : _nth_element_gen()
    {
        nth_container_type nths;
        nths.reserve(kth_percentiles.size());
        for (const auto &kth : kth_percentiles) {
            auto size = static_cast<size_t>(data_size);
            auto nth = static_cast<size_type>(kth_percentile_to_nth_index(size, kth));
            nths.push_back(nth);
        }

        _nth_element_gen.reset(
            new NthElementCaseGenerator(data_size, initial_value, range_size, nths));
    }

    ~percentile_case_generator() = default;

    // Call nth_element_case_generator internally to generate out-of-order `data` sized `data_size`
    // and nth elements. See nth_element_case_generator for detailed implementations.
    void operator()(container_type &data, container_type &elements)
    {
        (*_nth_element_gen)(data, elements);
    }

private:
    std::unique_ptr<NthElementCaseGenerator> _nth_element_gen;

    DISALLOW_COPY_AND_ASSIGN(percentile_case_generator);
};

template <typename T, typename = typename std::enable_if<std::is_integral<T>::value>::type>
using integral_percentile_case_generator =
    percentile_case_generator<integral_nth_element_case_generator<T>>;

template <typename T, typename = typename std::enable_if<std::is_floating_point<T>::value>::type>
using floating_percentile_case_generator =
    percentile_case_generator<floating_nth_element_case_generator<T>>;

} // namespace dsn
