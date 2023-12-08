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

#include <alloca.h>
#include <memory>
#include <vector>

#include "common/fs_manager.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "replica/mutation.h"
#include "replica/mutation_log.h"
#include "replica/replica.h"
#include "replica/replica_stub.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/task/task_spec.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/time_utils.h"

namespace dsn {
namespace replication {

DSN_DECLARE_int32(log_private_file_size_mb);

bool mutation_log_tool::dump(
    const std::string &log_dir,
    gpid pid,
    std::ostream &output,
    std::function<void(int64_t decree, int64_t timestamp, dsn::message_ex **requests, int count)>
        callback)
{
    std::string absolute_path;
    if (!utils::filesystem::get_absolute_path(log_dir, absolute_path)) {
        output << "ERROR: get absolute path failed" << std::endl;
        return false;
    }
    std::string norm_path;
    utils::filesystem::get_normalized_path(absolute_path, norm_path);
    auto dn = std::make_shared<dir_node>("", norm_path);
    app_info ai;
    ai.__set_app_type("pegasus");
    auto stub = std::make_shared<replica_stub>();
    auto *rep = new replica(stub.get(), pid, ai, dn.get(), false, false);
    auto mlog = std::make_shared<mutation_log_private>(
        log_dir, FLAGS_log_private_file_size_mb, pid, rep);
    error_code err = mlog->open(
        [mlog, &output, callback](int log_length, mutation_ptr &mu) -> bool {
            std::cout << "1" << std::endl;
            if (mlog->max_decree(mu->data.header.pid) == 0) {
                mlog->set_valid_start_offset_on_open(mu->data.header.pid, 0);
            }
            char timestamp_buf[32] = {0};
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
    delete rep;
    if (err != dsn::ERR_OK) {
        output << "ERROR: dump mutation log failed, err = " << err.to_string() << std::endl;
        return false;
    } else {
        return true;
    }
}
} // namespace replication
} // namespace dsn
