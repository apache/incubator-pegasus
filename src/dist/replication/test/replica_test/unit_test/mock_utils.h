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

#pragma once

#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/filesystem.h>

#include "dist/replication/lib/replica.h"
#include "dist/replication/lib/replica_stub.h"

namespace dsn {
namespace replication {

class mock_replication_app_base : public replication_app_base
{
public:
    explicit mock_replication_app_base(replica *replica) : replication_app_base(replica) {}

    error_code start(int, char **) override { return ERR_NOT_IMPLEMENTED; }
    error_code stop(bool) override { return ERR_NOT_IMPLEMENTED; }
    error_code sync_checkpoint() override { return ERR_NOT_IMPLEMENTED; }
    error_code async_checkpoint(bool) override { return ERR_NOT_IMPLEMENTED; }
    error_code prepare_get_checkpoint(blob &) override { return ERR_NOT_IMPLEMENTED; }
    error_code get_checkpoint(int64_t, const blob &, learn_state &) override
    {
        return ERR_NOT_IMPLEMENTED;
    }
    error_code storage_apply_checkpoint(chkpt_apply_mode, const learn_state &) override
    {
        return ERR_NOT_IMPLEMENTED;
    }
    error_code copy_checkpoint_to_dir(const char *, int64_t *) override
    {
        return ERR_NOT_IMPLEMENTED;
    }
    int on_request(message_ex *request) override { return 0; }
    std::string query_compact_state() const { return ""; };

    // we mock the followings
    void update_app_envs(const std::map<std::string, std::string> &envs) override { _envs = envs; }
    void query_app_envs(std::map<std::string, std::string> &out) override { out = _envs; }
    decree last_durable_decree() const override { return 0; }

private:
    std::map<std::string, std::string> _envs;
};

class mock_replica : public replica
{
public:
    mock_replica(replica_stub *stub, gpid gpid, const app_info &app, const char *dir)
        : replica(stub, gpid, app, dir, false)
    {
        _app = make_unique<replication::mock_replication_app_base>(this);
    }

    ~mock_replica() override
    {
        _config.status = partition_status::PS_INACTIVE;
        _app.reset(nullptr);
    }

    replica_duplicator_manager &get_replica_duplicator_manager() { return *_duplication_mgr; }

    void as_primary() { _config.status = partition_status::PS_PRIMARY; }

    void as_secondary() { _config.status = partition_status::PS_SECONDARY; }

    /// helper functions
    void set_replica_config(replica_configuration &config) { _config = config; }
    void set_partition_status(partition_status::type status) { _config.status = status; }
    void set_child_gpid(gpid pid) { _child_gpid = pid; }
};

inline std::unique_ptr<mock_replica> create_mock_replica(replica_stub *stub,
                                                         int appid = 1,
                                                         int partition_index = 1,
                                                         const char *dir = "./")
{
    gpid gpid(appid, partition_index);
    app_info app_info;
    app_info.app_type = "replica";
    app_info.app_name = "temp";

    return make_unique<mock_replica>(stub, gpid, app_info, dir);
}

class mock_replica_stub : public replica_stub
{
public:
    mock_replica_stub() : replica_stub() {}

    ~mock_replica_stub() override = default;

    void add_replica(replica *r) { _replicas[r->get_gpid()] = replica_ptr(r); }

    mock_replica *add_primary_replica(int appid, int part_index = 1)
    {
        auto r = add_non_primary_replica(appid, part_index);
        r->as_primary();
        return r;
    }

    mock_replica *add_non_primary_replica(int appid, int part_index = 1)
    {
        auto r = create_mock_replica(this, appid, part_index).release();
        add_replica(r);
        mock_replicas[gpid(appid, part_index)] = r;
        return r;
    }

    mock_replica *find_replica(int appid, int part_index = 1)
    {
        return mock_replicas[gpid(appid, part_index)];
    }

    void set_state_connected() { _state = replica_node_state::NS_Connected; }

    rpc_address get_meta_server_address() const override { return rpc_address("127.0.0.2", 12321); }

    std::map<gpid, mock_replica *> mock_replicas;

    /// helper functions
    std::unique_ptr<mock_replica>
    generate_replica(app_info info,
                     gpid pid,
                     partition_status::type status = partition_status::PS_INACTIVE,
                     ballot b = 5)
    {
        replica_configuration config;
        config.ballot = b;
        config.pid = pid;
        config.status = status;

        std::unique_ptr<mock_replica> rep =
            make_unique<mock_replica>(this, pid, std::move(info), "./");
        rep->set_replica_config(config);
        return rep;
    }
};

} // namespace replication
} // namespace dsn
