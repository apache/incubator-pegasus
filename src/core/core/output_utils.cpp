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

#include "dsn/utility/output_utils.h"

#include <dsn/c/api_utilities.h>

namespace dsn {
namespace utils {

template <typename Writer>
void json_encode(Writer &writer, const table_printer &tp)
{
    if (tp._matrix_data.empty()) {
        return;
    }
    if (!tp._name.empty()) {
        json::json_encode(writer, tp._name); // table_printer name
        writer.StartObject();
    }
    if (tp._mode == table_printer::data_mode::kMultiColumns) {
        // The 1st row elements are column names, skip it.
        for (size_t row = 1; row < tp._matrix_data.size(); ++row) {
            dsn::json::json_encode(writer, tp._matrix_data[row][0]); // row name
            writer.StartObject();
            for (int col = 0; col < tp._matrix_data[row].size(); col++) {
                dsn::json::json_encode(writer, tp._matrix_data[0][col]);   // column name
                dsn::json::json_encode(writer, tp._matrix_data[row][col]); // column data
            }
            writer.EndObject();
        }
    } else if (tp._mode == table_printer::data_mode::kSingleColumn) {
        for (size_t row = 0; row < tp._matrix_data.size(); ++row) {
            dsn::json::json_encode(writer, tp._matrix_data[row][0]); // row name
            dsn::json::json_encode(writer, tp._matrix_data[row][1]); // row data
        }
    } else {
        dassert(false, "Unknown mode");
    }
    if (!tp._name.empty()) {
        writer.EndObject();
    }
}

void table_printer::add_title(const std::string &title, alignment align)
{
    check_mode(data_mode::kMultiColumns);
    dassert(_matrix_data.empty() && _max_col_width.empty(), "`add_title` must be called only once");
    _max_col_width.push_back(title.length());
    _align_left.push_back(align == alignment::kLeft);
    add_row(title);
}

void table_printer::add_column(const std::string &col_name, alignment align)
{
    check_mode(data_mode::kMultiColumns);
    dassert(_matrix_data.size() == 1, "`add_column` must be called before real data appendding");
    _max_col_width.push_back(col_name.length());
    _align_left.push_back(align == alignment::kLeft);
    append_data(col_name);
}

void table_printer::add_row_name_and_string_data(const std::string &row_name,
                                                 const std::string &data)
{
    // The first row added to the table.
    if (_max_col_width.empty()) {
        _max_col_width.push_back(row_name.length());
        _align_left.push_back(true);
        _max_col_width.push_back(data.length());
        _align_left.push_back(true);
    }

    _matrix_data.emplace_back(std::vector<std::string>());
    append_string_data(row_name);
    append_string_data(data);
}

void table_printer::output(std::ostream &out, output_format format) const
{
    switch (format) {
    case output_format::kTabular:
        output_in_tabular(out);
        break;
    case output_format::kJsonCompact:
        output_in_json<dsn::json::JsonWriter>(out);
        break;
    case output_format::kJsonPretty:
        output_in_json<dsn::json::PrettyJsonWriter>(out);
        break;
    default:
        dassert(false, "Unknown format");
    }
}

void table_printer::output_in_tabular(std::ostream &out) const
{
    if (_max_col_width.empty()) {
        return;
    }

    std::string separator;
    if (_mode == data_mode::kSingleColumn) {
        separator = ": ";
    } else {
        dassert(_mode == data_mode::kMultiColumns, "Unknown mode");
    }

    if (!_name.empty()) {
        out << "[" << _name << "]" << std::endl;
    }
    int i = 0;
    for (const auto &row : _matrix_data) {
        for (size_t col = 0; col < row.size(); ++col) {
            auto data = (col == 0 ? "" : separator) + row[col];
            out << std::setw(_max_col_width[col] + _tabular_width)
                << (_align_left[col] ? std::left : std::right) << data;
        }
        out << std::endl;
    }
}

void table_printer::append_string_data(const std::string &data)
{
    _matrix_data.rbegin()->emplace_back(data);
    int last_index = _matrix_data.rbegin()->size() - 1;
    dassert(last_index <= _max_col_width.size(), "column data exceed");

    // update column max length
    int &cur_len = _max_col_width[last_index];
    if (cur_len < data.size()) {
        cur_len = data.size();
    }
}

void table_printer::check_mode(data_mode mode)
{
    if (_mode == data_mode::kUninitialized) {
        _mode = mode;
        return;
    }
    dassert(_mode == mode, "");
}

void multi_table_printer::add(table_printer &&tp) { _tps.emplace_back(std::move(tp)); }

void multi_table_printer::output(std::ostream &out,
                                 table_printer::table_printer::output_format format) const
{
    switch (format) {
    case table_printer::output_format::kTabular:
        output_in_tabular(out);
        break;
    case table_printer::output_format::kJsonCompact:
        output_in_json<dsn::json::JsonWriter>(out);
        break;
    case table_printer::output_format::kJsonPretty:
        output_in_json<dsn::json::PrettyJsonWriter>(out);
        break;
    default:
        dassert(false, "Unknown format");
    }
}

void multi_table_printer::output_in_tabular(std::ostream &out) const
{
    for (const auto &tp : _tps) {
        tp.output_in_tabular(out);
        out << std::endl;
    }
}

} // namespace utils
} // namespace dsn
