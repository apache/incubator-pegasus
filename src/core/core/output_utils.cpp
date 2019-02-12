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

void table_printer::add_title(const std::string &title, alignment align)
{
    check_mode(data_mode::KMultiColumns);
    dassert(matrix_data_.empty() && max_col_width_.empty(), "`add_title` must be called only once");
    max_col_width_.push_back(title.length());
    align_left_.push_back(align == alignment::kLeft);
    add_row(title);
}

void table_printer::add_column(const std::string &col_name, alignment align)
{
    check_mode(data_mode::KMultiColumns);
    dassert(matrix_data_.size() == 1, "`add_column` must be called before real data appendding");
    max_col_width_.emplace_back(col_name.length());
    align_left_.push_back(align == alignment::kLeft);
    append_data(col_name);
}

void table_printer::add_row_name_and_string_data(const std::string &row_name,
                                                 const std::string &data)
{
    // The first row added to the table.
    if (max_col_width_.empty()) {
        max_col_width_.push_back(row_name.length());
        align_left_.push_back(true);
        max_col_width_.push_back(data.length());
        align_left_.push_back(true);
    }

    matrix_data_.emplace_back(std::vector<std::string>());
    append_string_data(row_name);
    append_string_data(data);
}

void table_printer::output(std::ostream &out, const std::string &separator) const
{
    if (max_col_width_.empty()) {
        return;
    }

    for (const auto &row : matrix_data_) {
        for (size_t i = 0; i < row.size(); ++i) {
            auto data = (i == 0 ? "" : separator) + row[i];
            out << std::setw(max_col_width_[i] + space_width_)
                << (align_left_[i] ? std::left : std::right) << data;
        }
        out << std::endl;
    }
}

void table_printer::append_string_data(const std::string &data)
{
    matrix_data_.rbegin()->emplace_back(data);

    // update column max length
    int &cur_len = max_col_width_[matrix_data_.rbegin()->size() - 1];
    if (cur_len < data.size()) {
        cur_len = data.size();
    }
}

void table_printer::check_mode(data_mode mode)
{
    if (mode_ == data_mode::kUninitialized) {
        mode_ = mode;
        return;
    }
    dassert(mode_ == mode, "");
}

} // namespace utils
} // namespace dsn
