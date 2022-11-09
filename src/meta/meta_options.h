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

/*
 * Description:
 *     the meta server's options
 *
 * Revision history:
 *     2016-04-25, Weijie Sun(sunweijie at xiaomi.com), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
#pragma once

#include <string>
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"
#include "common/replication_other_types.h"
#include "common/replication.codes.h"

namespace dsn {
namespace replication {

class fd_suboptions
{
public:
    std::string distributed_lock_service_type;
    std::vector<std::string> distributed_lock_service_args;

    uint64_t stable_rs_min_running_seconds;
    int32_t max_succssive_unstable_restart;
};

class lb_suboptions
{
public:
    std::string server_load_balancer_type;
    uint64_t replica_assign_delay_ms_for_dropouts;
    int32_t max_replicas_in_group;

    bool balancer_in_turn;
    bool only_primary_balancer;
    bool only_move_primary;
};

class meta_options
{
public:
    std::string cluster_root;
    std::string meta_state_service_type;
    std::vector<std::string> meta_state_service_args;

    uint64_t node_live_percentage_threshold_for_update;
    meta_function_level::type meta_function_level_on_start;
    bool recover_from_replica_server;
    int32_t hold_seconds_for_dropped_app;

    bool add_secondary_enable_flow_control;
    int32_t add_secondary_max_count_for_one_node;

    fd_suboptions _fd_opts;
    lb_suboptions _lb_opts;
    std::string partition_guardian_type;

    bool cold_backup_disabled;

    bool enable_white_list;
    std::vector<std::string> replica_white_list;

public:
    void initialize();

public:
    static std::string concat_path_unix_style(const std::string &prefix,
                                              const std::string &postfix);
};
} // namespace replication
} // namespace dsn
