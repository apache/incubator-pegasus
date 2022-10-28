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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "simple_kv.server.impl.h"
#include <fstream>
#include <sstream>
#include "utils/filesystem.h"

namespace dsn {
namespace replication {
namespace application {

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
    std::string r;
    {
        zauto_lock l(_lock);

        auto it = _store.find(key);
        if (it != _store.end()) {
            r = it->second;
        }
    }

    LOG_DEBUG("read %s", r.c_str());
    reply(r);
}

// RPC_SIMPLE_KV_WRITE
void simple_kv_service_impl::on_write(const kv_pair &pr, ::dsn::rpc_replier<int32_t> &reply)
{
    {
        zauto_lock l(_lock);
        _store[pr.key] = pr.value;
    }

    LOG_DEBUG("write %s", pr.key.c_str());
    reply(0);
}

// RPC_SIMPLE_KV_APPEND
void simple_kv_service_impl::on_append(const kv_pair &pr, ::dsn::rpc_replier<int32_t> &reply)
{
    {
        zauto_lock l(_lock);
        auto it = _store.find(pr.key);
        if (it != _store.end())
            it->second.append(pr.value);
        else
            _store[pr.key] = pr.value;
    }

    LOG_DEBUG("append %s", pr.key.c_str());
    reply(0);
}

::dsn::error_code simple_kv_service_impl::start(int argc, char **argv)
{
    {
        zauto_lock l(_lock);
        set_last_durable_decree(0);
        recover();
    }
    return ERR_OK;
}

::dsn::error_code simple_kv_service_impl::stop(bool clear_state)
{
    {
        zauto_lock l(_lock);
        if (clear_state) {
            CHECK(dsn::utils::filesystem::remove_path(_dir_data),
                  "Fail to delete directory {}",
                  _dir_data);
            reset_state();
        }
    }

    return ERR_OK;
}

// checkpoint related
void simple_kv_service_impl::recover()
{
    zauto_lock l(_lock);

    _store.clear();

    int64_t maxVersion = 0;
    std::string name;

    std::vector<std::string> sub_list;
    std::string path = _dir_data;
    CHECK(dsn::utils::filesystem::get_subfiles(path, sub_list, false),
          "Fail to get subfiles in {}",
          path);
    for (auto &fpath : sub_list) {
        auto &&s = dsn::utils::filesystem::get_file_name(fpath);
        if (s.substr(0, strlen("checkpoint.")) != std::string("checkpoint."))
            continue;

        int64_t version = static_cast<int64_t>(atoll(s.substr(strlen("checkpoint.")).c_str()));
        if (version > maxVersion) {
            maxVersion = version;
            name = std::string(data_dir()) + "/" + s;
        }
    }
    sub_list.clear();

    if (maxVersion > 0) {
        recover(name, maxVersion);
        set_last_durable_decree(maxVersion);
    }
}

void simple_kv_service_impl::recover(const std::string &name, int64_t version)
{
    zauto_lock l(_lock);

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
    is.close();
}

::dsn::error_code simple_kv_service_impl::sync_checkpoint()
{
    char name[256];
    int64_t last_commit = _last_committed_decree.load();
    sprintf(name, "%s/checkpoint.%" PRId64, _dir_data.c_str(), last_commit);

    zauto_lock l(_lock);

    if (last_commit == last_durable_decree()) {
        CHECK(utils::filesystem::file_exists(name), "checkpoint file {} is missing!", name);
        return ERR_OK;
    }

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

    os.close();

    // TODO: gc checkpoints
    set_last_durable_decree(last_commit);
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
    if (last_durable_decree() > 0) {
        char name[256];
        sprintf(name, "%s/checkpoint.%" PRId64, _dir_data.c_str(), last_durable_decree());

        state.from_decree_excluded = 0;
        state.to_decree_included = last_durable_decree();
        state.files.push_back(std::string(name));
        return ERR_OK;
    } else {
        state.from_decree_excluded = 0;
        state.to_decree_included = 0;
        return ERR_OBJECT_NOT_FOUND;
    }
}

::dsn::error_code simple_kv_service_impl::storage_apply_checkpoint(chkpt_apply_mode mode,
                                                                   const learn_state &state)
{
    if (mode == chkpt_apply_mode::learn) {
        recover(state.files[0], state.to_decree_included);
        return ERR_OK;
    } else {
        CHECK_EQ_MSG(chkpt_apply_mode::copy, mode, "invalid mode");
        CHECK_GT(state.to_decree_included, last_durable_decree());

        char name[256];
        sprintf(name, "%s/checkpoint.%" PRId64, _dir_data.c_str(), state.to_decree_included);
        std::string lname(name);

        if (!utils::filesystem::rename_path(state.files[0], lname))
            return ERR_CHECKPOINT_FAILED;
        else {
            set_last_durable_decree(state.to_decree_included);
            return ERR_OK;
        }
    }
}
}
}
} // namespace
