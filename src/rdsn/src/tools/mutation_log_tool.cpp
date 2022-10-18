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

#include "mutation_log_tool.h"
#include "utils/time_utils.h"
#include "replica/mutation_log.h"

namespace dsn {
namespace replication {

bool mutation_log_tool::dump(
    const std::string &log_dir,
    std::ostream &output,
    std::function<void(int64_t decree, int64_t timestamp, dsn::message_ex **requests, int count)>
        callback)
{
    mutation_log_ptr mlog = new mutation_log_shared(log_dir, 32, false);
    error_code err = mlog->open(
        [mlog, &output, callback](int log_length, mutation_ptr &mu) -> bool {
            if (mlog->max_decree(mu->data.header.pid) == 0) {
                mlog->set_valid_start_offset_on_open(mu->data.header.pid, 0);
            }
            char timestamp_buf[32];
            utils::time_ms_to_string(mu->data.header.timestamp / 1000, timestamp_buf);
            output << "mutation [" << mu->name() << "]: "
                   << "gpid=" << mu->data.header.pid.get_app_id() << "."
                   << mu->data.header.pid.get_partition_index() << ", "
                   << "ballot=" << mu->data.header.ballot << ", decree=" << mu->data.header.decree
                   << ", "
                   << "timestamp=" << timestamp_buf
                   << ", last_committed_decree=" << mu->data.header.last_committed_decree << ", "
                   << "log_offset=" << mu->data.header.log_offset << ", log_length=" << log_length
                   << ", "
                   << "update_count=" << mu->data.updates.size();
            if (callback && mu->data.updates.size() > 0) {

                dsn::message_ex **batched_requests =
                    (dsn::message_ex **)alloca(sizeof(dsn::message_ex *) * mu->data.updates.size());
                int batched_count = 0;
                for (mutation_update &update : mu->data.updates) {
                    dsn::message_ex *req = dsn::message_ex::create_received_request(
                        update.code,
                        (dsn_msg_serialize_format)update.serialization_type,
                        (void *)update.data.data(),
                        update.data.length());
                    batched_requests[batched_count++] = req;
                }
                callback(mu->data.header.decree,
                         mu->data.header.timestamp,
                         batched_requests,
                         batched_count);
                for (int i = 0; i < batched_count; i++) {
                    batched_requests[i]->release_ref();
                }
            }
            return true;
        },
        nullptr);
    mlog->close();
    if (err != dsn::ERR_OK) {
        output << "ERROR: dump mutation log failed, err = " << err.to_string() << std::endl;
        return false;
    } else {
        return true;
    }
}
} // namespace replication
} // namespace dsn
