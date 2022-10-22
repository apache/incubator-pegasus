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

#include "utils/output_utils.h"

#include <gtest/gtest.h>

#include <vector>
#include <string>

using std::vector;
using std::string;
using dsn::utils::table_printer;

namespace dsn {

const vector<string>
    single_column_tp_output({"[tp1]\n"
                             "row1  : 1.23\n"
                             "row2  : 2345\n"
                             "row3  : 3456\n",
                             R"*("tp1":{"row1":"1.23","row2":"2345","row3":"3456"})*",
                             R"*(    "tp1": {)*"
                             "\n"
                             R"*(        "row1": "1.23",)*"
                             "\n"
                             R"*(        "row2": "2345",)*"
                             "\n"
                             R"*(        "row3": "3456")*"
                             "\n"
                             "    }"});

const vector<string> multi_columns_tp_output(
    {"[tp2]\n"
     "multi_columns_test  col0    col1    col2    \n"
     "row0                data00  data01  data02  \n"
     "row1                data10  data11  data12  \n"
     "row2                data20  data21  data22  \n",
     R"*("tp2":{"row0":{"multi_columns_test":"row0","col0":"data00","col1":"data01","col2":"data02"},"row1":{"multi_columns_test":"row1","col0":"data10","col1":"data11","col2":"data12"},"row2":{"multi_columns_test":"row2","col0":"data20","col1":"data21","col2":"data22"}})*",
     R"*(    "tp2": {)*"
     "\n"
     R"*(        "row0": {)*"
     "\n"
     R"*(            "multi_columns_test": "row0",)*"
     "\n"
     R"*(            "col0": "data00",)*"
     "\n"
     R"*(            "col1": "data01",)*"
     "\n"
     R"*(            "col2": "data02")*"
     "\n"
     "        },\n"
     R"*(        "row1": {)*"
     "\n"
     R"*(            "multi_columns_test": "row1",)*"
     "\n"
     R"*(            "col0": "data10",)*"
     "\n"
     R"*(            "col1": "data11",)*"
     "\n"
     R"*(            "col2": "data12")*"
     "\n"
     "        },\n"
     R"*(        "row2": {)*"
     "\n"
     R"*(            "multi_columns_test": "row2",)*"
     "\n"
     R"*(            "col0": "data20",)*"
     "\n"
     R"*(            "col1": "data21",)*"
     "\n"
     R"*(            "col2": "data22")*"
     "\n"
     "        }\n"
     "    }"});

utils::table_printer generate_single_column_tp()
{
    utils::table_printer tp("tp1", 2, 2);
    tp.add_row_name_and_data("row1", 1.234);
    tp.add_row_name_and_data("row2", 2345);
    tp.add_row_name_and_data("row3", "3456");
    return tp;
}

utils::table_printer generate_multi_columns_tp()
{
    int kColumnCount = 3;
    int kRowCount = 3;
    utils::table_printer tp("tp2", 2, 2);
    tp.add_title("multi_columns_test");
    for (int i = 0; i < kColumnCount; i++) {
        tp.add_column("col" + std::to_string(i));
    }
    for (int i = 0; i < kRowCount; i++) {
        tp.add_row("row" + std::to_string(i));
        for (int j = 0; j < kColumnCount; j++) {
            tp.append_data("data" + std::to_string(i) + std::to_string(j));
        }
    }
    return tp;
}

template <typename P>
void check_output(const P &printer, const vector<string> &expect_output)
{
    static vector<table_printer::output_format> output_formats(
        {table_printer::output_format::kTabular,
         table_printer::output_format::kJsonCompact,
         table_printer::output_format::kJsonPretty});
    ASSERT_EQ(expect_output.size(), output_formats.size());
    for (int i = 0; i < output_formats.size(); i++) {
        std::ostringstream out;
        printer.output(out, output_formats[i]);
        ASSERT_EQ(expect_output[i], out.str());
    }
}

TEST(table_printer_test, empty_content_test)
{
    utils::table_printer tp;
    ASSERT_NO_FATAL_FAILURE(check_output(tp, {"", "{}\n", "{}\n"}));
}

TEST(table_printer_test, empty_name_test)
{
    utils::table_printer tp;
    tp.add_row_name_and_data("row1", 1.234);
    ASSERT_NO_FATAL_FAILURE(check_output(tp,
                                         {"row1  : 1.23\n",
                                          R"*({"row1":"1.23"})*"
                                          "\n",
                                          "{\n"
                                          R"*(    "row1": "1.23")*"
                                          "\n}\n"}));
}

TEST(table_printer_test, single_column_test)
{
    utils::table_printer tp(generate_single_column_tp());
    ASSERT_NO_FATAL_FAILURE(check_output(tp,
                                         {single_column_tp_output[0],
                                          "{" + single_column_tp_output[1] + "}\n",
                                          "{\n" + single_column_tp_output[2] + "\n}\n"}));
}

TEST(table_printer_test, multi_columns_test)
{
    utils::table_printer tp(generate_multi_columns_tp());
    ASSERT_NO_FATAL_FAILURE(check_output(tp,
                                         {multi_columns_tp_output[0],
                                          "{" + multi_columns_tp_output[1] + "}\n",
                                          "{\n" + multi_columns_tp_output[2] + "\n}\n"}));
}

TEST(multi_table_printer_test, empty_content_test)
{
    utils::multi_table_printer mtp;
    ASSERT_NO_FATAL_FAILURE(check_output(mtp, {"", "{}\n", "{}\n"}));
}

TEST(multi_table_printer_test, single_empty_sub_test)
{
    utils::multi_table_printer mtp;
    utils::table_printer tp;
    mtp.add(std::move(tp));
    ASSERT_NO_FATAL_FAILURE(check_output(mtp, {"\n", "{}\n", "{}\n"}));
}

TEST(multi_table_printer_test, multi_sub_test)
{
    utils::multi_table_printer mtp;
    mtp.add(generate_single_column_tp());
    mtp.add(generate_multi_columns_tp());
    ASSERT_NO_FATAL_FAILURE(check_output(
        mtp,
        {single_column_tp_output[0] + "\n" + multi_columns_tp_output[0] + "\n",
         "{" + single_column_tp_output[1] + "," + multi_columns_tp_output[1] + "}\n",
         "{\n" + single_column_tp_output[2] + ",\n" + multi_columns_tp_output[2] + "\n}\n"}));
}
} // namespace dsn
