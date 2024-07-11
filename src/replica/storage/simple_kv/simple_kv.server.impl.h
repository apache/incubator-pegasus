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

#include <stdint.h>
#include <map>
#include <string>

#include "metadata_types.h"
#include "replica/replication_app_base.h"
#include "simple_kv.server.h"
#include "utils/error_code.h"
#include "utils/zlocks.h"

namespace dsn {
class blob;
template <typename TResponse>
class rpc_replier;

namespace replication {
class learn_state;
class replica;

namespace application {
class kv_pair;

class simple_kv_service_impl : public simple_kv_service
{
public:
    static void register_service()
    {
        replication_app_base::register_storage_engine(
            "simple_kv", replication_app_base::create<simple_kv_service_impl>);
        simple_kv_service::register_rpc_handlers();
    }
    simple_kv_service_impl(replica *r);

    // RPC_SIMPLE_KV_READ
    virtual void on_read(const std::string &key, ::dsn::rpc_replier<std::string> &reply);
    // RPC_SIMPLE_KV_WRITE
    virtual void on_write(const kv_pair &pr, ::dsn::rpc_replier<int32_t> &reply);
    // RPC_SIMPLE_KV_APPEND
    virtual void on_append(const kv_pair &pr, ::dsn::rpc_replier<int32_t> &reply);

    virtual ::dsn::error_code start(int argc, char **argv) override;

    virtual ::dsn::error_code stop(bool cleanup = false) override;

    int64_t last_flushed_decree() const override { return _last_durable_decree; }

    int64_t last_durable_decree() const override { return _last_durable_decree; }

    virtual ::dsn::error_code sync_checkpoint() override;

    virtual ::dsn::error_code async_checkpoint(bool flush_memtable) override;

    virtual ::dsn::error_code copy_checkpoint_to_dir(const char *checkpoint_dir,
                                                     int64_t *last_decree,
                                                     bool flush_memtable = false) override
    {
        return ERR_NOT_IMPLEMENTED;
    }

    virtual ::dsn::error_code prepare_get_checkpoint(blob &learn_req) { return dsn::ERR_OK; }

    virtual ::dsn::error_code get_checkpoint(int64_t learn_start,
                                             const dsn::blob &learn_request,
                                             /*out*/ learn_state &state) override;

    virtual ::dsn::error_code storage_apply_checkpoint(chkpt_apply_mode mode,
                                                       const learn_state &state) override;

    std::string query_compact_state() const override { return ""; }

    virtual void update_app_envs(const std::map<std::string, std::string> &envs) {}

    virtual void query_app_envs(/*out*/ std::map<std::string, std::string> &envs) {}

    virtual uint32_t query_data_version() const override { return 0; }

    virtual ::dsn::replication::manual_compaction_status::type query_compact_status() const override
    {
        return dsn::replication::manual_compaction_status::IDLE;
    }

private:
    void recover();
    void recover(const std::string &name, int64_t version);
    void set_last_durable_decree(int64_t d) { _last_durable_decree = d; }

    void reset_state();

private:
    typedef std::map<std::string, std::string> simple_kv;
    zlock _lock;
    simple_kv _store;
    int64_t _last_durable_decree;
};
} // namespace application
} // namespace replication
} // namespace dsn
