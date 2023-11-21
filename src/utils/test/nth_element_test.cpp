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

#include "utils/nth_element.h"

#include <fmt/core.h>
#include <cstdint>

#include "gtest/gtest.h"
#include "nth_element_utils.h"

namespace dsn {

template <typename NthElementFinder,
          typename = typename std::enable_if<
              std::is_integral<typename NthElementFinder::value_type>::value>::type>
void run_integral_cases(const typename NthElementFinder::container_type &array,
                        const typename NthElementFinder::nth_container_type &nths,
                        const typename NthElementFinder::container_type &expected_elements)
{
    auto container = array;

    NthElementFinder finder;
    finder.set_nths(nths);
    finder(container.begin(), container.begin(), container.end());

    ASSERT_EQ(finder.elements(), expected_elements);
}

template <typename NthElementFinder,
          typename = typename std::enable_if<
              std::is_integral<typename NthElementFinder::value_type>::value>::type>
void run_basic_int64_cases()
{
    // Test cases:
    // - both the array and the nth list are empty
    // - the array has only one element, and the nth list is empty
    // - the array has only one element, and the nth list has only one element
    // - the array has only one element, and the nth list has duplicate elements
    // - the array has only 2 identical elements, and the nth list has only one element
    // - the array has only 2 identical elements, and the nth list has both elements
    // - the array has only 2 identical elements, and the nth list has duplicat elements
    // - the array has only 2 ordered elements, and the nth list has only one element
    // - the array has only 2 ordered elements, and the nth list has both elements
    // - the array has only 2 ordered elements, and the nth list has duplicat elements
    // - the array has only 2 unordered elements, and the nth list has only one element
    // - the array has only 2 unordered elements, and the nth list has both elements
    // - the array has only 2 unordered elements, and the nth list has duplicat elements
    // - the array contains identical elements, and the nth list has only one element
    // - the array contains identical elements, and the nth list has all elements
    // - the array contains identical elements, and the nth list has duplicat elements
    // - all elements in the array are identical, and the nth list has 2 elements
    // - all elements in the array are identical, and the nth list has all elements
    // - all elements in the array are identical, and the nth list has duplicat elements
    // - each element in the array is different from others, and the nth list has 3 elements
    // - each element in the array is different from others, and the nth list has all elements
    // - each element in the array is different from others, and the nth list has duplicat elements
    struct test_case
    {
        typename NthElementFinder::container_type array;
        typename NthElementFinder::nth_container_type nths;
        typename NthElementFinder::container_type expected_elements;
    } tests[] = {{{}, {}, {}},
                 {{1}, {}, {}},
                 {{1}, {0}, {1}},
                 {{1}, {0, 0}, {1, 1}},
                 {{1, 1}, {1}, {1}},
                 {{1, 1}, {0, 1}, {1, 1}},
                 {{1, 1}, {1, 1}, {1, 1}},
                 {{1, 2}, {1}, {2}},
                 {{1, 2}, {0, 1}, {1, 2}},
                 {{1, 2}, {1, 1}, {2, 2}},
                 {{2, 1}, {1}, {2}},
                 {{2, 1}, {0, 1}, {1, 2}},
                 {{2, 1}, {0, 0}, {1, 1}},
                 {{2, 1, 2, 3, 2}, {2}, {2}},
                 {{2, 1, 2, 3, 2}, {0, 1, 2, 3, 4}, {1, 2, 2, 2, 3}},
                 {{2, 1, 2, 3, 2}, {0, 0, 2, 2, 3, 3}, {1, 1, 2, 2, 2, 2}},
                 {{2, 2, 2, 2, 2, 2}, {2, 3}, {2, 2}},
                 {{2, 2, 2, 2, 2, 2}, {0, 1, 2, 3, 4, 5}, {2, 2, 2, 2, 2, 2}},
                 {{2, 2, 2, 2, 2, 2}, {1, 1, 2, 2, 5, 5}, {2, 2, 2, 2, 2, 2}},
                 {{5, 6, 2, 8, 1, 7}, {3, 4, 5}, {6, 7, 8}},
                 {{5, 6, 2, 8, 1, 7}, {0, 1, 2, 3, 4, 5}, {1, 2, 5, 6, 7, 8}},
                 {{5, 6, 2, 8, 1, 7}, {0, 0, 2, 2, 5, 5}, {1, 1, 5, 5, 8, 8}}};

    for (const auto &test : tests) {
        run_integral_cases<NthElementFinder>(test.array, test.nths, test.expected_elements);
    }
}

TEST(nth_element_test, basic_int64) { run_basic_int64_cases<stl_nth_element_finder<int64_t>>(); }

template <typename NthElementFinder>
void run_generated_int64_cases()
{
    // Test cases:
    // - generate empty array with empty nth list
    // - generate an array of only one element with the nth list of only one element
    // - generate an array of 2 elements with the nth list of 2 elements
    // - generate an array of 5000 elements with the nth list of 8 elements, at range size 2
    // - generate an array of 5000 elements with the nth list of 8 elements, at range size 5
    // - generate an array of 5000 elements with the nth list of 8 elements, at range size 10
    // - generate an array of 5000 elements with the nth list of 8 elements, at range size 100
    // - generate an array of 5000 elements with the nth list of 8 elements, at range size 10000
    // - generate an array of 5000 elements with duplicate nth elements, at range size 10000
    struct test_case
    {
        typename NthElementFinder::size_type array_size;
        int64_t initial_value;
        uint64_t range_size;
        typename NthElementFinder::nth_container_type nths;
    } tests[] = {{0, 0, 2, {}},
                 {1, 0, 2, {0}},
                 {2, 0, 2, {0, 1}},
                 {5000, 0, 2, {999, 1999, 2499, 2999, 3499, 3999, 4499, 4999}},
                 {5000, 0, 5, {999, 1999, 2499, 2999, 3499, 3999, 4499, 4999}},
                 {5000, 0, 10, {999, 1999, 2499, 2999, 3499, 3999, 4499, 4999}},
                 {5000, 0, 100, {999, 1999, 2499, 2999, 3499, 3999, 4499, 4999}},
                 {5000, 0, 10000, {999, 1999, 2499, 2999, 3499, 3999, 4499, 4999}},
                 {5000, 0, 10000, {999, 999, 2999, 2999, 3999, 3999, 4999, 4999}}};

    for (const auto &test : tests) {
        integral_nth_element_case_generator<int64_t> generator(
            test.array_size, test.initial_value, test.range_size, test.nths);

        integral_nth_element_case_generator<int64_t>::container_type array;
        integral_nth_element_case_generator<int64_t>::container_type expected_elements;
        generator(array, expected_elements);

        run_integral_cases<NthElementFinder>(array, test.nths, expected_elements);
    }
}

TEST(nth_element_test, generated_int64)
{
    run_generated_int64_cases<stl_nth_element_finder<int64_t>>();
}

template <typename NthElementFinder,
          typename = typename std::enable_if<
              std::is_floating_point<typename NthElementFinder::value_type>::value>::type>
void run_floating_cases(const typename NthElementFinder::container_type &array,
                        const typename NthElementFinder::nth_container_type &nths,
                        const typename NthElementFinder::container_type &expected_elements)
{
    auto container = array;

    NthElementFinder finder;
    finder.set_nths(nths);
    finder(container.begin(), container.begin(), container.end());

    ASSERT_EQ(finder.elements().size(), expected_elements.size());
    for (typename NthElementFinder::size_type i = 0; i < finder.elements().size(); ++i) {
        ASSERT_DOUBLE_EQ(finder.elements()[i], expected_elements[i]);
    }
}

template <typename NthElementFinder,
          typename = typename std::enable_if<
              std::is_floating_point<typename NthElementFinder::value_type>::value>::type>
void run_basic_double_cases()
{
    // Test cases:
    // - both the array and the nth list are empty
    // - the array has only one element, and the nth list is empty
    // - the array has only one element, and the nth list has only one element
    // - the array has only one element, and the nth list has duplicate elements
    // - the array has only 2 identical elements, and the nth list has only one element
    // - the array has only 2 identical elements, and the nth list has both elements
    // - the array has only 2 identical elements, and the nth list has duplicat elements
    // - the array has only 2 ordered elements, and the nth list has only one element
    // - the array has only 2 ordered elements, and the nth list has both elements
    // - the array has only 2 ordered elements, and the nth list has duplicat elements
    // - the array has only 2 unordered elements, and the nth list has only one element
    // - the array has only 2 unordered elements, and the nth list has both elements
    // - the array has only 2 unordered elements, and the nth list has duplicat elements
    // - the array contains identical elements, and the nth list has only one element
    // - the array contains identical elements, and the nth list has all elements
    // - the array contains identical elements, and the nth list has duplicat elements
    // - all elements in the array are identical, and the nth list has 2 elements
    // - all elements in the array are identical, and the nth list has all elements
    // - all elements in the array are identical, and the nth list has duplicat elements
    // - each element in the array is different from others, and the nth list has 3 elements
    // - each element in the array is different from others, and the nth list has all elements
    struct test_case
    {
        typename NthElementFinder::container_type array;
        typename NthElementFinder::nth_container_type nths;
        typename NthElementFinder::container_type expected_elements;
    } tests[] = {
        {{}, {}, {}},
        {{1.23}, {}, {}},
        {{1.23}, {0}, {1.23}},
        {{1.23}, {0, 0}, {1.23, 1.23}},
        {{1.23, 1.23}, {1}, {1.23}},
        {{1.23, 1.23}, {0, 1}, {1.23, 1.23}},
        {{1.23, 1.23}, {1, 1}, {1.23, 1.23}},
        {{1.23, 2.34}, {1}, {2.34}},
        {{1.23, 2.34}, {0, 1}, {1.23, 2.34}},
        {{1.23, 2.34}, {1, 1}, {2.34, 2.34}},
        {{2.34, 1.23}, {1}, {2.34}},
        {{2.34, 1.23}, {0, 1}, {1.23, 2.34}},
        {{2.34, 1.23}, {0, 0}, {1.23, 1.23}},
        {{2.34, 1.23, 2.34, 3.56, 2.34}, {2}, {2.34}},
        {{2.34, 1.23, 2.34, 3.56, 2.34}, {0, 1, 2, 3, 4}, {1.23, 2.34, 2.34, 2.34, 3.56}},
        {{2.34, 1.23, 2.34, 3.56, 2.34}, {0, 0, 2, 2, 3, 3}, {1.23, 1.23, 2.34, 2.34, 2.34, 2.34}},
        {{2.34, 2.34, 2.34, 2.34, 2.34, 2.34}, {2, 3}, {2.34, 2.34}},
        {{2.34, 2.34, 2.34, 2.34, 2.34, 2.34},
         {0, 1, 2, 3, 4, 5},
         {2.34, 2.34, 2.34, 2.34, 2.34, 2.34}},
        {{2.34, 2.34, 2.34, 2.34, 2.34, 2.34},
         {1, 1, 2, 2, 5, 5},
         {2.34, 2.34, 2.34, 2.34, 2.34, 2.34}},
        {{5.67, 6.78, 2.34, 8.90, 1.23, 7.89}, {3, 4, 5}, {6.78, 7.89, 8.90}},
        {{5.67, 6.78, 2.34, 8.90, 1.23, 7.89},
         {0, 1, 2, 3, 4, 5},
         {1.23, 2.34, 5.67, 6.78, 7.89, 8.90}},
        {{5.67, 6.78, 2.34, 8.90, 1.23, 7.89},
         {0, 0, 2, 2, 5, 5},
         {1.23, 1.23, 5.67, 5.67, 8.90, 8.90}}};

    for (const auto &test : tests) {
        run_floating_cases<NthElementFinder>(test.array, test.nths, test.expected_elements);
    }
}

TEST(nth_element_test, basic_double) { run_basic_double_cases<stl_nth_element_finder<double>>(); }

template <typename NthElementFinder>
void run_generated_double_cases()
{
    // Test cases:
    // - generate empty array with empty nth list
    // - generate an array of only one element with the nth list of only one element
    // - generate an array of 2 elements with the nth list of 2 elements
    // - generate an array of 5000 elements with the nth list of 8 elements, at range size 2
    // - generate an array of 5000 elements with the nth list of 8 elements, at range size 5
    // - generate an array of 5000 elements with the nth list of 8 elements, at range size 10
    // - generate an array of 5000 elements with the nth list of 8 elements, at range size 100
    // - generate an array of 5000 elements with the nth list of 8 elements, at range size 10000
    // - generate an array of 5000 elements with duplicate nth elements, at range size 10000
    struct test_case
    {
        typename NthElementFinder::size_type array_size;
        double initial_value;
        uint64_t range_size;
        typename NthElementFinder::nth_container_type nths;
    } tests[] = {{0, 0.0, 2, {}},
                 {1, 0.0, 2, {0}},
                 {2, 0.0, 2, {0, 1}},
                 {5000, 0.0, 2, {999, 1999, 2499, 2999, 3499, 3999, 4499, 4999}},
                 {5000, 0.0, 5, {999, 1999, 2499, 2999, 3499, 3999, 4499, 4999}},
                 {5000, 0.0, 10, {999, 1999, 2499, 2999, 3499, 3999, 4499, 4999}},
                 {5000, 0.0, 100, {999, 1999, 2499, 2999, 3499, 3999, 4499, 4999}},
                 {5000, 0.0, 10000, {999, 1999, 2499, 2999, 3499, 3999, 4499, 4999}},
                 {5000, 0.0, 10000, {999, 999, 2999, 2999, 3999, 3999, 4999, 4999}}};

    for (const auto &test : tests) {
        floating_nth_element_case_generator<double> generator(
            test.array_size, test.initial_value, test.range_size, test.nths);

        floating_nth_element_case_generator<double>::container_type array;
        floating_nth_element_case_generator<double>::container_type expected_elements;
        generator(array, expected_elements);

        run_floating_cases<NthElementFinder>(array, test.nths, expected_elements);
    }
}

TEST(nth_element_test, generated_double)
{
    run_generated_double_cases<stl_nth_element_finder<double>>();
}

} // namespace dsn
