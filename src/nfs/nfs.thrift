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

include "../../idl/dsn.thrift"

namespace cpp dsn.service

struct copy_request
{
    1: dsn.rpc_address         source;
    2: string                  source_dir;
    3: string                  dst_dir;
    4: string                  file_name;
    5: i64                     offset;
    6: i32                     size;
    7: bool                    is_last;
    8: bool                    overwrite;
    9: optional string         source_disk_tag;
    10: optional dsn.gpid      pid;
    11: optional dsn.host_port hp_source;
}

struct copy_response
{
    1: dsn.error_code error;
    2: dsn.blob file_content;
    3: i64 offset;
    4: i32 size;
}

struct get_file_size_request
{
    1: dsn.rpc_address        source;
    2: string                 dst_dir;
    3: list<string>           file_list;
    4: string                 source_dir;
    5: bool                   overwrite;
    6: optional string        source_disk_tag;
    7: optional string        dest_disk_tag;
    8: optional dsn.gpid      pid;
    9: optional dsn.host_port hp_source;
}

struct get_file_size_response
{
    1: i32 error;
    2: list<string> file_list;
    3: list<i64> size_list;
}
