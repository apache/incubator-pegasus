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
#include "simple_kv.server.impl.h"
#include <fstream>
#include <sstream>
#include "utils/filesystem.h"

#define VALUE_NOT_EXIST "<<not-exist>>"

namespace dsn {
namespace replication {
namespace test {

bool simple_kv_service_impl::s_simple_kv_open_fail = false;
bool simple_kv_service_impl::s_simple_kv_close_fail = false;
bool simple_kv_service_impl::s_simple_kv_get_checkpoint_fail = false;
bool simple_kv_service_impl::s_simple_kv_apply_checkpoint_fail = false;

simple_kv_service_impl::simple_kv_service_impl(replica *r) : simple_kv_service(r), _lock(true)
{
    reset_state();
    LOG_INFO("simple_kv_service_impl inited");
}

void simple_kv_service_impl::reset_state()
{
    _test_file_learning = dsn_config_get_value_bool("test", "test_file_learning", true, "");
    _last_durable_decree = 0;
}

// RPC_SIMPLE_KV_READ
void simple_kv_service_impl::on_read(const std::string &key, ::dsn::rpc_replier<std::string> &reply)
{
    dsn::zauto_lock l(_lock);

    std::string value;
    auto it = _store.find(key);
    if (it == _store.end()) {
        value = VALUE_NOT_EXIST;
    } else {
        value = it->second;
    }

    // LOG_INFO("=== on_exec_read:int64_t=%" PRId64 ",key=%s,value=%s", last_committed_decree(),
    // key.c_str(), value.c_str());
    reply(value);
}

// RPC_SIMPLE_KV_WRITE
void simple_kv_service_impl::on_write(const kv_pair &pr, ::dsn::rpc_replier<int32_t> &reply)
{
    dsn::zauto_lock l(_lock);
    _store[pr.key] = pr.value;

    // LOG_INFO("=== on_exec_write:int64_t=%" PRId64 ",key=%s,value=%s", last_committed_decree(),
    // pr.key.c_str(), pr.value.c_str());
    reply(0);
}

// RPC_SIMPLE_KV_APPEND
void simple_kv_service_impl::on_append(const kv_pair &pr, ::dsn::rpc_replier<int32_t> &reply)
{
    dsn::zauto_lock l(_lock);
    auto it = _store.find(pr.key);
    if (it != _store.end())
        it->second.append(pr.value);
    else
        _store[pr.key] = pr.value;

    // LOG_INFO("=== on_exec_append:int64_t=%" PRId64 ",key=%s,value=%s", last_committed_decree(),
    // pr.key.c_str(), pr.value.c_str());
    reply(0);
}

::dsn::error_code simple_kv_service_impl::start(int argc, char **argv)
{
    if (s_simple_kv_open_fail) {
        return ERR_CORRUPTION;
    }

    dsn::zauto_lock l(_lock);
    recover();
    LOG_INFO("simple_kv_service_impl opened");
    return ERR_OK;
}

::dsn::error_code simple_kv_service_impl::stop(bool clear_state)
{
    if (s_simple_kv_close_fail) {
        return ERR_CORRUPTION;
    }

    dsn::zauto_lock l(_lock);
    if (clear_state) {
        CHECK(dsn::utils::filesystem::remove_path(data_dir()),
              "Fail to delete directory {}",
              data_dir());
        _store.clear();
        reset_state();
    }
    LOG_INFO("simple_kv_service_impl closed, clear_state = %s", clear_state ? "true" : "false");
    return ERR_OK;
}

// checkpoint related
void simple_kv_service_impl::recover()
{
    dsn::zauto_lock l(_lock);

    _store.clear();

    int64_t max_version = 0;
    std::string name;

    std::vector<std::string> sub_list;
    std::string path = data_dir();
    CHECK(dsn::utils::filesystem::get_subfiles(path, sub_list, false),
          "Fail to get subfiles in {}",
          path);
    for (auto &fpath : sub_list) {
        auto &&s = dsn::utils::filesystem::get_file_name(fpath);
        if (s.substr(0, strlen("checkpoint.")) != std::string("checkpoint."))
            continue;

        int64_t version = static_cast<int64_t>(atoll(s.substr(strlen("checkpoint.")).c_str()));
        if (version > max_version) {
            max_version = version;
            name = std::string(data_dir()) + "/" + s;
        }
    }
    sub_list.clear();

    if (max_version > 0) {
        recover(name, max_version);
        set_last_durable_decree(max_version);
    }
    LOG_INFO("simple_kv_service_impl recovered, last_durable_decree = %" PRId64 "",
             last_durable_decree());
}

void simple_kv_service_impl::recover(const std::string &name, int64_t version)
{
    dsn::zauto_lock l(_lock);

    std::ifstream is(name.c_str(), std::ios::binary);
    if (!is.is_open())
        return;

    _store.clear();

    uint64_t count;
    int magic;

    is.read((char *)&count, sizeof(count));
    is.read((char *)&magic, sizeof(magic));
    CHECK_EQ_MSG(magic, 0xdeadbeef, "invalid checkpoint");

    for (uint64_t i = 0; i < count; i++) {
        std::string key;
        std::string value;

        uint32_t sz;
        is.read((char *)&sz, (uint32_t)sizeof(sz));
        key.resize(sz);

        is.read((char *)&key[0], sz);

        is.read((char *)&sz, (uint32_t)sizeof(sz));
        value.resize(sz);

        is.read((char *)&value[0], sz);

        _store[key] = value;
    }
}

::dsn::error_code simple_kv_service_impl::sync_checkpoint()
{
    dsn::zauto_lock l(_lock);

    int64_t last_commit = last_committed_decree();
    if (last_commit == last_durable_decree()) {
        LOG_INFO("simple_kv_service_impl no need to create checkpoint, "
                 "checkpoint already the latest, last_durable_decree = %" PRId64 "",
                 last_durable_decree());
        return ERR_OK;
    }

    // TODO: should use async write instead
    char name[256];
    sprintf(name, "%s/checkpoint.%" PRId64, data_dir().c_str(), last_commit);
    std::ofstream os(name, std::ios::binary);

    uint64_t count = (uint64_t)_store.size();
    int magic = 0xdeadbeef;

    os.write((const char *)&count, (uint32_t)sizeof(count));
    os.write((const char *)&magic, (uint32_t)sizeof(magic));

    for (auto it = _store.begin(); it != _store.end(); ++it) {
        const std::string &k = it->first;
        uint32_t sz = (uint32_t)k.length();

        os.write((const char *)&sz, (uint32_t)sizeof(sz));
        os.write((const char *)&k[0], sz);

        const std::string &v = it->second;
        sz = (uint32_t)v.length();

        os.write((const char *)&sz, (uint32_t)sizeof(sz));
        os.write((const char *)&v[0], sz);
    }

    set_last_durable_decree(last_commit);
    LOG_INFO("simple_kv_service_impl create checkpoint succeed, "
             "last_durable_decree = %" PRId64 "",
             last_durable_decree());
    return ERR_OK;
}

::dsn::error_code simple_kv_service_impl::async_checkpoint(bool flush_memtable)
{
    return sync_checkpoint();
}

// helper routines to accelerate learning
::dsn::error_code simple_kv_service_impl::get_checkpoint(int64_t learn_start,
                                                         const dsn::blob &learn_request,
                                                         /*out*/ learn_state &state)
{
    if (s_simple_kv_get_checkpoint_fail) {
        return ERR_CORRUPTION;
    }

    if (last_durable_decree() == 0) {
        sync_checkpoint();
    }

    if (last_durable_decree() > 0) {
        char name[256];
        sprintf(name, "%s/checkpoint.%" PRId64, data_dir().c_str(), last_durable_decree());

        state.from_decree_excluded = 0;
        state.to_decree_included = last_durable_decree();
        state.files.push_back(std::string(name));

        LOG_INFO("simple_kv_service_impl get checkpoint succeed, last_durable_decree = %" PRId64 "",
                 last_durable_decree());
        return ERR_OK;
    } else {
        state.from_decree_excluded = 0;
        state.to_decree_included = 0;
        LOG_ERROR("simple_kv_service_impl get checkpoint failed, no checkpoint found");
        return ERR_OBJECT_NOT_FOUND;
    }
}

::dsn::error_code simple_kv_service_impl::storage_apply_checkpoint(chkpt_apply_mode mode,
                                                                   const learn_state &state)
{
    if (s_simple_kv_apply_checkpoint_fail) {
        return ERR_CORRUPTION;
    }

    if (mode == replication_app_base::chkpt_apply_mode::learn) {
        recover(state.files[0], state.to_decree_included);
        // LOG_INFO("simple_kv_service_impl learn checkpoint succeed, last_committed_decree = %"
        // PRId64 "", last_committed_decree());
        return ERR_OK;
    } else {
        CHECK_EQ_MSG(replication_app_base::chkpt_apply_mode::copy, mode, "invalid mode");
        CHECK_GT(state.to_decree_included, last_durable_decree());

        char name[256];
        sprintf(name, "%s/checkpoint.%" PRId64, data_dir().c_str(), state.to_decree_included);
        std::string lname(name);

        if (!utils::filesystem::rename_path(state.files[0], lname)) {
            LOG_ERROR("simple_kv_service_impl copy checkpoint failed, rename path failed");
            return ERR_CHECKPOINT_FAILED;
        } else {
            set_last_durable_decree(state.to_decree_included);
            LOG_INFO(
                "simple_kv_service_impl copy checkpoint succeed, last_durable_decree = %" PRId64 "",
                last_durable_decree());
            return ERR_OK;
        }
    }
}
}
}
}
