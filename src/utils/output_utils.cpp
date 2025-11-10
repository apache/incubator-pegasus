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

#include "utils/output_utils.h"

#include <cstdlib>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <memory>

#include "common/json_helper.h"
#include "utils/fmt_logging.h"

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
        CHECK(false, "Unknown mode");
    }
    if (!tp._name.empty()) {
        writer.EndObject();
    }
}

void table_printer::add_title(const std::string &title, alignment align)
{
    check_mode(data_mode::kMultiColumns);
    CHECK(_matrix_data.empty() && _max_col_width.empty(), "'add_title' must be called only once");
    _max_col_width.push_back(title.length());
    _align_left.push_back(align == alignment::kLeft);
    add_row(title);
}

void table_printer::add_column(const std::string &col_name, alignment align)
{
    check_mode(data_mode::kMultiColumns);
    CHECK_EQ_MSG(_matrix_data.size(), 1, "'add_column' must be called before real data appendding");
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
        CHECK(false, "Unknown format");
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
        CHECK(_mode == data_mode::kMultiColumns, "Unknown mode");
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
    CHECK_LE_MSG(last_index, _max_col_width.size(), "column data exceed");

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
    CHECK(_mode == mode, "");
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
        CHECK(false, "Unknown format");
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
