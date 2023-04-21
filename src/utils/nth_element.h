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

#include <fmt/format.h>
#include <algorithm>
#include <functional>
#include <limits>
#include <type_traits>
#include <vector>

#include "utils/fmt_logging.h"
#include "utils/ports.h"

namespace dsn {

// The finder helps to find multiple nth elements of a sequence container (e.g. std::vector)
// at a time, based on nth_element() of STL.
template <typename T, typename Compare = std::less<T>>
class stl_nth_element_finder
{
public:
    using value_type = T;
    using container_type = std::vector<value_type>;
    using size_type = typename container_type::size_type;
    using nth_container_type = std::vector<size_type>;

    stl_nth_element_finder(const Compare &comp = Compare()) : _nths(), _elements(), _comp(comp) {}

    // Set with specified nth indexes. An nth index is typically an index of the sequence
    // container (e.g. std::vector). This method allows nth indexes to be updated dynamically.
    //
    // There are 2 reasons why both `_nths` and `_elements` are put into the sequence container:
    //
    // (1) The users of stl_nth_element_finder, such as the metric of percentile, may pass
    // duplicate nth indexes to `_nths`. For example, suppose that the sampled window size is
    // 100, both P99 and P999 will have the same nth element -- namely 99th element. Thus it's
    // will be convenient for users if `nths` can contain duplicate elements.
    //
    // The sequence container can contain duplicate elements, even if all elements in the container
    // are sorted. Therefore, there may be identical indexes in `nths`.
    //
    // (2) The sequence container is more cache-friendly. While an nth element is selected, it's
    // cache-friendly to write it into `_elements`. After all nth elements are collected into
    // `_elements`, scanning them (`elements()`) is also cache-friendly, even if there are many
    // nth indexes in `_nths`. In contrast to this, access directly to the nth element in array
    // will not be cache-friendly especially when the array is large.
    //
    // Notice that the indexes in `nths` list must be ordered. After `operator()` is executed,
    // the elements returned by `elements()` will be in the order of the sorted nth indexes.
    void set_nths(const nth_container_type &nths)
    {
        _nths = nths;
        CHECK(std::is_sorted(_nths.begin(), _nths.end()),
              "nth indexes({}) is not sorted",
              fmt::join(_nths, " "));

        _elements.assign(_nths.size(), value_type{});
    }

    // Find the multiple nth elements.
    //
    // Typically `begin` is the beginning iterator of the sequence container. `begin` plus each
    // member of `_nths` will be the real nth element of the sequence container.
    //
    // [first, last) is the real range for finding the multiple nth elements.
    template <typename RandomAccessIterator>
    void
    operator()(RandomAccessIterator begin, RandomAccessIterator first, RandomAccessIterator last)
    {
        for (size_type i = 0; i < _nths.size();) {
            auto nth_iter = begin + _nths[i];
            CHECK(nth_iter >= first && nth_iter < last, "Invalid iterators for nth_element()");
            std::nth_element(first, nth_iter, last, _comp);
            _elements[i] = *nth_iter;

            // Identical nth indexes should be processed. See `set_nths()` for details.
            for (++i; i < _nths.size() && _nths[i] == _nths[i - 1]; ++i) {
                _elements[i] = *nth_iter;
            }

            first = nth_iter + 1;
        }
    }

    const container_type &elements() const { return _elements; }

private:
    nth_container_type _nths;
    container_type _elements;
    Compare _comp;

    DISALLOW_COPY_AND_ASSIGN(stl_nth_element_finder);
};

template <typename T, typename = typename std::enable_if<std::is_floating_point<T>::value>::type>
class floating_comparator
{
public:
    bool operator()(const T &lhs, const T &rhs) const
    {
        return rhs - lhs >= std::numeric_limits<T>::epsilon();
    }
};

template <typename T, typename = typename std::enable_if<std::is_floating_point<T>::value>::type>
using floating_stl_nth_element_finder = stl_nth_element_finder<T, floating_comparator<T>>;

} // namespace dsn
