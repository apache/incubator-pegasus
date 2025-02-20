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

#include <memory>
#include <vector>

#include "common/fs_manager.h"
#include "common/gpid.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "fmt/core.h"
#include "replica/mutation.h"
#include "replica/mutation_log.h"
#include "replica/replica.h"
#include "replica/replica_stub.h"
#include "rpc/rpc_message.h"
#include "task/task_spec.h"
#include "utils/alloc.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/defer.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/time_utils.h"

DSN_DECLARE_int32(log_private_file_size_mb);

namespace dsn {
namespace replication {

bool mutation_log_tool::dump(
    const std::string &log_dir,
    gpid pid,
    std::ostream &output,
    std::function<void(int64_t decree, int64_t timestamp, dsn::message_ex **requests, int count)>
        callback)
{
    std::string absolute_path;
    if (!utils::filesystem::get_absolute_path(log_dir, absolute_path)) {
        output << fmt::format("ERROR: get absolute path failed\n");
        return false;
    }
    std::string norm_path;
    utils::filesystem::get_normalized_path(absolute_path, norm_path);
    auto dn = std::make_unique<dir_node>(/* tag_ */ "", norm_path);
    app_info ai;
    ai.__set_app_type("pegasus");
    auto stub = std::make_unique<replica_stub>();
    // Constructor of replica is private which can not be accessed by std::make_unique, so use raw
    // pointer here.
    auto *rep = new replica(stub.get(),
                            pid,
                            ai,
                            dn.get(),
                            /* need_restore */ false,
                            /* is_duplication_follower */ false);
    auto cleanup = dsn::defer([rep]() { delete rep; });
    auto mlog =
        std::make_shared<mutation_log_private>(log_dir, FLAGS_log_private_file_size_mb, pid, rep);
    error_code err = mlog->open(
        [mlog, &output, callback](int log_length, mutation_ptr &mu) -> bool {
            if (mlog->max_decree(mu->data.header.pid) == 0) {
                mlog->set_valid_start_offset_on_open(mu->data.header.pid, 0);
            }
            char timestamp_buf[32] = {0};
            utils::time_ms_to_string(mu->data.header.timestamp / 1000, timestamp_buf);
            output << fmt::format("mutation [{}]: gpid={}, ballot={}, decree={}, timestamp={}, "
                                  "last_committed_decree={}, log_offset={}, log_length={}, "
                                  "update_count={}\n",
                                  mu->name(),
                                  mu->data.header.pid,
                                  mu->data.header.ballot,
                                  mu->data.header.decree,
                                  timestamp_buf,
                                  mu->data.header.last_committed_decree,
                                  mu->data.header.log_offset,
                                  log_length,
                                  mu->data.updates.size());
            if (callback && !mu->data.updates.empty()) {
                auto **batched_requests = ALLOC_STACK(message_ex *, mu->data.updates.size());
                int batched_count = 0;
                for (mutation_update &update : mu->data.updates) {
                    dsn::message_ex *req = dsn::message_ex::create_received_request(
                        update.code,
                        static_cast<dsn_msg_serialize_format>(update.serialization_type),
                        update.data.data(),
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
        output << fmt::format("ERROR: dump mutation log failed, err = {}\n", err);
        return false;
    } else {
        return true;
    }
}
} // namespace replication
} // namespace dsn
