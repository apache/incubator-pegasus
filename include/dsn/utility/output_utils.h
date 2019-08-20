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

#include <dsn/cpp/json_helper.h>

namespace dsn {
namespace utils {

class table_printer;
class multi_table_printer;

// Keep the same code style with dsn/cpp/json_helper.h
template <typename Writer>
void json_encode(Writer &out, const table_printer &tp);

/// A tool used to print data in a table form.
///
/// Example usage 1:
///    table_printer tp("sample_data");
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
///    tp.output(out, output_format::kTabular);
///
/// Output looks like:
///    [sample_data]
///    table_title  column_name1  column_name2
///    row_name_1   123           45.67
///    row_name_2   456           45.68
///
/// Example usage 2:
///    table_printer tp("sample_data_2");
///    tp.add_row_name_and_data("row_name_1", int_value);
///    tp.add_row_name_and_data("row_name_2", string_value);
///
///    std::ostream out(...);
///    tp.output(out);
///
/// Output looks like:
///    [sample_data_2]
///    row_name_1 :  4567
///    row_name_2 :  hello
///
class table_printer
{
private:
    enum class data_mode
    {
        kUninitialized = 0,
        kSingleColumn = 1,
        kMultiColumns = 2
    };

public:
    enum class alignment
    {
        kLeft = 0,
        kRight = 1,
    };

    enum class output_format
    {
        kTabular = 0,
        kJsonCompact = 1,
        kJsonPretty = 2,
    };

    explicit table_printer(std::string name = "", int tabular_width = 2, int precision = 2)
        : _name(std::move(name)),
          _mode(data_mode::kUninitialized),
          _tabular_width(tabular_width),
          _precision(precision)
    {
    }

    // kMultiColumns mode.
    void add_title(const std::string &title, alignment align = alignment::kLeft);
    void add_column(const std::string &col_name, alignment align = alignment::kLeft);
    template <typename T>
    void add_row(const T &row_name)
    {
        check_mode(data_mode::kMultiColumns);
        _matrix_data.emplace_back(std::vector<std::string>());
        append_data(row_name);
    }
    template <typename T>
    void append_data(const T &data)
    {
        check_mode(data_mode::kMultiColumns);
        append_string_data(to_string(data));
    }

    // kSingleColumn mode.
    template <typename T>
    void add_row_name_and_data(const std::string &row_name, const T &data)
    {
        check_mode(data_mode::kSingleColumn);
        add_row_name_and_string_data(row_name, to_string(data));
    }

    // Output result.
    void output(std::ostream &out, output_format format = output_format::kTabular) const;

private:
    template <typename T>
    std::string to_string(T data)
    {
        return std::to_string(data);
    }

    void check_mode(data_mode mode);

    void append_string_data(const std::string &data);
    void add_row_name_and_string_data(const std::string &row_name, const std::string &data);

    void output_in_tabular(std::ostream &out) const;
    template <typename Writer>
    void output_in_json(std::ostream &out) const
    {
        rapidjson::OStreamWrapper wrapper(out);
        Writer writer(wrapper);
        writer.StartObject();
        json_encode(writer, *this);
        writer.EndObject();
        out << std::endl;
    }

private:
    friend class multi_table_printer;
    template <typename Writer>
    friend void json_encode(Writer &out, const table_printer &tp);

    std::string _name;
    data_mode _mode;
    int _tabular_width;
    int _precision;
    std::vector<bool> _align_left;
    std::vector<int> _max_col_width;
    std::vector<std::vector<std::string>> _matrix_data;
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
        s << std::fixed << std::setprecision(_precision) << data;
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

// Helper to output multiple tables into one large table.
class multi_table_printer
{
public:
    void add(table_printer &&tp);
    void output(std::ostream &out, table_printer::output_format format) const;

private:
    void output_in_tabular(std::ostream &out) const;
    template <typename Writer>
    void output_in_json(std::ostream &out) const
    {
        rapidjson::OStreamWrapper wrapper(out);
        Writer writer(wrapper);
        writer.StartObject();
        for (const auto &tp : _tps) {
            json_encode(writer, tp);
        }
        writer.EndObject();
        out << std::endl;
    }

private:
    std::vector<table_printer> _tps;
};
} // namespace utils
} // namespace dsn
