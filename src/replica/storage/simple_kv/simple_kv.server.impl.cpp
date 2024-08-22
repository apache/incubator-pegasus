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

#include <fmt/core.h>
#include <inttypes.h>
#include <rocksdb/slice.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <atomic>
#include <memory>
#include <utility>
#include <vector>

#include "aio/aio_task.h"
#include "aio/file_io.h"
#include "common/replication.codes.h"
#include "consensus_types.h"
#include "replica/storage/simple_kv/simple_kv.server.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "runtime/serverlet.h"
#include "simple_kv_types.h"
#include "utils/autoref_ptr.h"
#include "utils/binary_reader.h"
#include "utils/blob.h"
#include "utils/env.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/utils.h"

namespace dsn {

namespace replication {
class replica;

namespace application {

simple_kv_service_impl::simple_kv_service_impl(replica *r) : simple_kv_service(r), _lock(true)
{
    reset_state();
    LOG_INFO("simple_kv_service_impl inited");
}

void simple_kv_service_impl::reset_state() { _last_durable_decree = 0; }

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

    LOG_DEBUG("read {}", r);
    reply(r);
}

// RPC_SIMPLE_KV_WRITE
void simple_kv_service_impl::on_write(const kv_pair &pr, ::dsn::rpc_replier<int32_t> &reply)
{
    {
        zauto_lock l(_lock);
        _store[pr.key] = pr.value;
    }

    LOG_DEBUG("write {}", pr.key);
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

    LOG_DEBUG("append {}", pr.key);
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

    std::unique_ptr<rocksdb::SequentialFile> rfile;
    auto s = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive)
                 ->NewSequentialFile(name, &rfile, rocksdb::EnvOptions());
    CHECK(s.ok(), "open log file '{}' failed, err = {}", name, s.ToString());

    _store.clear();

    // Read header.
    uint64_t count = 0;
    int magic = 0;
    rocksdb::Slice result;
    static const uint64_t kHeaderSize = sizeof(count) + sizeof(magic);
    char buff[kHeaderSize] = {0};
    s = rfile->Read(kHeaderSize, &result, buff);
    CHECK(s.ok(), "read header failed, err = {}", s.ToString());
    CHECK(!result.empty(), "read EOF of file '{}'", name);

    binary_reader reader(blob(buff, 0, kHeaderSize));
    CHECK_EQ(sizeof(count), reader.read(count));
    CHECK_EQ(sizeof(magic), reader.read(magic));
    CHECK_EQ_MSG(magic, 0xdeadbeef, "invalid checkpoint");

    // Read kv pairs.
    for (uint64_t i = 0; i < count; i++) {
        // Read key.
        uint32_t sz = 0;
        s = rfile->Read(sizeof(sz), &result, (char *)&sz);
        CHECK(s.ok(), "read key size failed, err = {}", s.ToString());
        CHECK(!result.empty(), "read EOF of file '{}'", name);

        std::shared_ptr<char> key_buffer(dsn::utils::make_shared_array<char>(sz));
        s = rfile->Read(sz, &result, key_buffer.get());
        CHECK(s.ok(), "read key failed, err = {}", s.ToString());
        CHECK(!result.empty(), "read EOF of file '{}'", name);
        std::string key = result.ToString();

        // Read value.
        s = rfile->Read(sizeof(sz), &result, (char *)&sz);
        CHECK(s.ok(), "read value size failed, err = {}", s.ToString());
        CHECK(!result.empty(), "read EOF of file '{}'", name);

        std::shared_ptr<char> value_buffer(dsn::utils::make_shared_array<char>(sz));
        s = rfile->Read(sz, &result, value_buffer.get());
        CHECK(s.ok(), "read value failed, err = {}", s.ToString());
        CHECK(!result.empty(), "read EOF of file '{}'", name);
        std::string value = result.ToString();

        // Store the kv pair.
        _store[key] = value;
    }
}

::dsn::error_code simple_kv_service_impl::sync_checkpoint()
{
    int64_t last_commit = _last_committed_decree.load();
    std::string fname = fmt::format("{}/checkpoint.{}", data_dir(), last_commit);

    zauto_lock l(_lock);
    if (last_commit == last_durable_decree()) {
        CHECK(utils::filesystem::file_exists(fname), "checkpoint file {} is missing!", fname);
        return ERR_OK;
    }

    auto wfile = file::open(fname, file::FileOpenType::kWriteOnly);
    CHECK_NOTNULL(wfile, "");

#define WRITE_DATA_SIZE(data, size)                                                                \
    do {                                                                                           \
        auto tsk = ::dsn::file::write(                                                             \
            wfile, (char *)&data, size, offset, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr);     \
        tsk->wait();                                                                               \
        offset += size;                                                                            \
    } while (false)

#define WRITE_DATA(data) WRITE_DATA_SIZE(data, sizeof(data))

    uint64_t offset = 0;
    uint64_t count = (uint64_t)_store.size();
    WRITE_DATA(count);

    int magic = 0xdeadbeef;
    WRITE_DATA(magic);

    for (const auto &kv : _store) {
        const std::string &k = kv.first;
        uint32_t sz = (uint32_t)k.length();
        WRITE_DATA(sz);
        WRITE_DATA_SIZE(k[0], sz);

        const std::string &v = kv.second;
        sz = (uint32_t)v.length();
        WRITE_DATA(sz);
        WRITE_DATA_SIZE(v[0], sz);
    }
#undef WRITE_DATA
#undef WRITE_DATA_SIZE

    CHECK_EQ(ERR_OK, file::flush(wfile));
    CHECK_EQ(ERR_OK, file::close(wfile));

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
} // namespace application
} // namespace replication
} // namespace dsn
