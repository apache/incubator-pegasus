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

std::shared_ptr<app_state> app_state::create(const std::string &name, const std::string &type, int32_t id)
{
    std::shared_ptr<app_state> result = std::make_shared<app_state>();

    result->is_stateful = true;
    result->max_replica_count = 3;

    result->app_name = name;
    result->app_type = type;
    result->app_id = id;
    result->status = app_status::AS_CREATING;
    result->helpers->owner = result.get();
    return result;
}

std::shared_ptr<app_state> app_state::create(const app_info& app_info)
{
    std::shared_ptr<app_state> result = std::make_shared<app_state>(app_info);
    result->helpers->owner = result.get();
    result->init_partitions(app_info.partition_count, app_info.max_replica_count);
    return result;
}

void app_state::init_partitions(int32_t pc, int32_t rc)
{
    partition_count = pc;
    max_replica_count = rc;

    partition_configuration config;
    config.ballot = 0;
    config.pid.set_app_id(app_id);
    config.last_committed_decree = 0;
    config.last_drops.clear();
    config.max_replica_count = rc;
    config.primary.set_invalid();
    config.secondaries.clear();
    partitions.assign(pc, config);
    for (int i=0; i!=pc; ++i)
        partitions[i].pid.set_partition_index(i);

    helpers->on_init_partitions();
}

}}
