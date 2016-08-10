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
 *     the meta server's date structure, impl file
 *
 * Revision history:
 *     2016-04-25, Weijie Sun(sunweijie at xiaomi.com), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
#include <dsn/service_api_cpp.h>
#include "meta_data.h"

namespace dsn { namespace replication {

void maintain_drops(/*inout*/ std::vector<rpc_address>& drops, const rpc_address& node, bool is_add)
{
    auto it = std::find(drops.begin(), drops.end(), node);
    if (is_add)
    {
        if (it != drops.end())
            drops.erase(it);
    }
    else
    {
        if (it == drops.end())
        {
            drops.push_back(node);
            if (drops.size() > 3)
                drops.erase(drops.begin());
        }
        else
        {
            dassert(false, "the node cannot be in drops set before this update", node.to_string());
        }
    }
}

void config_context::cancel_sync()
{
    if (config_status::pending_remote_sync == stage)
    {
        pending_sync_task->cancel(false);
        pending_sync_task = nullptr;
        pending_sync_request.reset();
    }
    if (msg)
    {
        dsn_msg_release_ref(msg);
    }
    msg = nullptr;
    stage = config_status::not_pending;
}

void config_context::clear_proposal()
{
    if (config_status::pending_remote_sync != stage)
        stage = config_status::not_pending;
    balancer_proposal.reset();
}

void app_state_helper::on_init_partitions()
{
    config_context context;
    context.stage = config_status::not_pending;
    context.pending_sync_task = nullptr;
    context.msg = nullptr;
    contexts.assign(owner->partition_count, context);
}

app_state::app_state(const app_info &info): app_info(info), helpers(new app_state_helper())
{
    helpers->owner = this;

    partition_configuration config;
    config.ballot = 0;
    config.pid.set_app_id(app_id);
    config.last_committed_decree = 0;
    config.last_drops.clear();
    config.max_replica_count = app_info::max_replica_count;
    config.primary.set_invalid();
    config.secondaries.clear();
    partitions.assign(app_info::partition_count, config);
    for (int i=0; i!=app_info::partition_count; ++i)
        partitions[i].pid.set_partition_index(i);

    helpers->on_init_partitions();
}

std::shared_ptr<app_state> app_state::create(const app_info& info)
{
    std::shared_ptr<app_state> result = std::make_shared<app_state>(info);
    result->helpers->owner = result.get();
    return result;
}

}}
