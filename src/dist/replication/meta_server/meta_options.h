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
#include <dsn/dist/replication.h>
#include <dsn/dist/replication/replication.types.h>

namespace dsn { namespace replication {

class meta_options
{
public:
    std::string cluster_root;
    std::string meta_state_service_type;
    std::string distributed_lock_service_type;
    std::string server_load_balancer_type;

    std::vector<std::string> meta_state_service_args;
    std::vector<std::string> distributed_lock_service_args;

    uint64_t replica_assign_delay_ms_for_dropouts;
    uint64_t node_live_percentage_threshold_for_update;
    uint64_t min_live_node_count_for_unfreeze;
public:
    void initialize();

public:
    static std::string concat_path_unix_style(const std::string& prefix, const std::string& postfix);
};

}}
