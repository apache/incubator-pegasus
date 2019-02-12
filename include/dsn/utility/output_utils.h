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

#pragma once

#include <cmath>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace dsn {
namespace utils {

/// A tool used to print data in a table form.
///
/// Example usage 1:
///    table_printer tp;
///    tp.add_title("table_title");
///    tp.add_column("column_name1");
///    tp.add_column("column_name2");
///    for (...) {
///        tp.add_row("row_name_i");
///        tp.append_data(int_data);
///        tp.append_data(double_data);
///    }
///
///    std::ostream out(...);
///    tp.output(out);
///
/// Output looks like:
///    table_title  column_name1  column_name2
///    row_name_1   123           45.67
///    row_name_2   456           45.68
///
/// Example usage 2:
///    table_printer tp;
///    tp.add_row_name_and_data("row_name_1", int_value);
///    tp.add_row_name_and_data("row_name_2", string_value);
///
///    std::ostream out(...);
///    tp.output(out, ": ");
///
/// Output looks like:
///    row_name_1 :  4567
///    row_name_2 :  hello
///
class table_printer
{
private:
    enum class data_mode
    {
        kUninitialized = 0,
        KSingleColumn = 1,
        KMultiColumns = 2
    };

public:
    enum class alignment
    {
        kLeft = 0,
        kRight = 1,
    };

    table_printer(int space_width = 2, int precision = 2)
        : mode_(data_mode::kUninitialized), space_width_(space_width), precision_(precision)
    {
    }

    // KMultiColumns
    void add_title(const std::string &title, alignment align = alignment::kLeft);
    void add_column(const std::string &col_name, alignment align = alignment::kLeft);
    template <typename T>
    void add_row(const T &row_name)
    {
        check_mode(data_mode::KMultiColumns);
        matrix_data_.emplace_back(std::vector<std::string>());
        append_data(row_name);
    }
    template <typename T>
    void append_data(const T &data)
    {
        check_mode(data_mode::KMultiColumns);
        append_string_data(to_string(data));
    }

    // KSingleColumn
    template <typename T>
    void add_row_name_and_data(const std::string &row_name, const T &data)
    {
        check_mode(data_mode::KSingleColumn);
        add_row_name_and_string_data(row_name, to_string(data));
    }

    void output(std::ostream &out, const std::string &separator = "") const;

private:
    template <typename T>
    std::string to_string(T data)
    {
        return std::to_string(data);
    }

    void check_mode(data_mode mode);
    void append_string_data(const std::string &data);
    void add_row_name_and_string_data(const std::string &row_name, const std::string &data);

private:
    data_mode mode_;
    int space_width_;
    int precision_;
    std::vector<bool> align_left_;
    std::vector<int> max_col_width_;
    std::vector<std::vector<std::string>> matrix_data_;
};

template <>
inline std::string table_printer::to_string<bool>(bool data)
{
    return data ? "true" : "false";
}

template <>
inline std::string table_printer::to_string<double>(double data)
{
    if (std::abs(data) < 1e-6) {
        return "0.00";
    } else {
        std::stringstream s;
        s << std::fixed << std::setprecision(precision_) << data;
        return s.str();
    }
}

template <>
inline std::string table_printer::to_string<std::string>(std::string data)
{
    return data;
}

template <>
inline std::string table_printer::to_string<const char *>(const char *data)
{
    return std::string(data);
}

} // namespace utils
} // namespace dsn
