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

#include "replica.h"
#include "mutation.h"
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/utility/factory_store.h>
#include <dsn/utility/filesystem.h>
#include <dsn/utility/crc.h>
#include <dsn/service_api_c.h>
#include <dsn/utility/smart_pointers.h>
#include <fstream>
#include <sstream>
#include <memory>

namespace dsn {
namespace replication {

error_code replica_init_info::load(const std::string &dir)
{
    std::string old_info_path = utils::filesystem::path_combine(dir, ".info");
    std::string new_info_path = utils::filesystem::path_combine(dir, ".init-info");
    std::string load_path;
    error_code err;
    if (utils::filesystem::path_exists(new_info_path)) {
        load_path = new_info_path;
        err = load_json(new_info_path.c_str());
    } else {
        load_path = old_info_path;
        err = load_binary(old_info_path.c_str());
    }

    if (err == ERR_OK) {
        ddebug(
            "load replica_init_info from %s succeed: %s", load_path.c_str(), to_string().c_str());
        if (load_path == old_info_path) {
            // change replica_init_info file from old to new
            err = store_json(new_info_path.c_str());
            if (err == ERR_OK) {
                ddebug("change replica_init_info file from %s to %s succeed",
                       old_info_path.c_str(),
                       new_info_path.c_str());
                utils::filesystem::remove_path(old_info_path);
            } else {
                derror("change replica_init_info file from %s to %s failed, err = %s",
                       old_info_path.c_str(),
                       new_info_path.c_str(),
                       err.to_string());
            }
        }
    } else {
        derror(
            "load replica_init_info from %s failed, err = %s", load_path.c_str(), err.to_string());
    }

    return err;
}

error_code replica_init_info::store(const std::string &dir)
{
    uint64_t start = dsn_now_ns();
    std::string new_info_path = utils::filesystem::path_combine(dir, ".init-info");
    error_code err = store_json(new_info_path.c_str());
    if (err == ERR_OK) {
        std::string old_info_path = utils::filesystem::path_combine(dir, ".info");
        if (utils::filesystem::file_exists(old_info_path)) {
            utils::filesystem::remove_path(old_info_path);
        }
        ddebug("store replica_init_info to %s succeed, time_used_ns = %" PRIu64 ": %s",
               new_info_path.c_str(),
               dsn_now_ns() - start,
               to_string().c_str());
    } else {
        derror("store replica_init_info to %s failed, time_used_ns = %" PRIu64 ", err = %s",
               new_info_path.c_str(),
               dsn_now_ns() - start,
               err.to_string());
    }

    return err;
}

error_code replica_init_info::load_binary(const char *file)
{
    std::ifstream is(file, std::ios::binary);
    if (!is.is_open()) {
        derror("open file %s failed", file);
        return ERR_FILE_OPERATION_FAILED;
    }

    magic = 0x0;
    is.read((char *)this, sizeof(*this));
    is.close();

    if (magic != 0xdeadbeef) {
        derror("data in file %s is invalid (magic)", file);
        return ERR_INVALID_DATA;
    }

    auto fcrc = crc;
    crc = 0; // set for crc computation
    auto lcrc = dsn::utils::crc32_calc((const void *)this, sizeof(*this), 0);
    crc = fcrc; // recover

    if (lcrc != fcrc) {
        derror("data in file %s is invalid (crc)", file);
        return ERR_INVALID_DATA;
    }

    return ERR_OK;
}

error_code replica_init_info::store_binary(const char *file)
{
    std::string ffile = std::string(file);
    std::string tmp_file = ffile + ".tmp";

    std::ofstream os(tmp_file.c_str(),
                     (std::ofstream::out | std::ios::binary | std::ofstream::trunc));
    if (!os.is_open()) {
        derror("open file %s failed", tmp_file.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    // compute crc
    crc = 0;
    crc = dsn::utils::crc32_calc((const void *)this, sizeof(*this), 0);

    os.write((const char *)this, sizeof(*this));
    os.close();

    if (!utils::filesystem::rename_path(tmp_file, ffile)) {
        derror("move file from %s to %s failed", tmp_file.c_str(), ffile.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    return ERR_OK;
}

error_code replica_init_info::load_json(const char *file)
{
    std::ifstream is(file, std::ios::binary);
    if (!is.is_open()) {
        derror("open file %s failed", file);
        return ERR_FILE_OPERATION_FAILED;
    }

    int64_t sz = 0;
    if (!::dsn::utils::filesystem::file_size(std::string(file), sz)) {
        derror("get file size of %s failed", file);
        return ERR_FILE_OPERATION_FAILED;
    }

    std::shared_ptr<char> buffer(dsn::utils::make_shared_array<char>(sz));
    is.read((char *)buffer.get(), sz);
    if (is.bad()) {
        derror("read file %s failed", file);
        return ERR_FILE_OPERATION_FAILED;
    }
    is.close();

    if (!dsn::json::json_forwarder<replica_init_info>::decode(blob(buffer, sz), *this)) {
        derror("decode json from file %s failed", file);
        return ERR_FILE_OPERATION_FAILED;
    }

    return ERR_OK;
}

error_code replica_init_info::store_json(const char *file)
{
    std::string ffile = std::string(file);
    std::string tmp_file = ffile + ".tmp";

    std::ofstream os(tmp_file.c_str(),
                     (std::ofstream::out | std::ios::binary | std::ofstream::trunc));
    if (!os.is_open()) {
        derror("open file %s failed", tmp_file.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    dsn::blob bb = dsn::json::json_forwarder<replica_init_info>::encode(*this);
    os.write((const char *)bb.data(), (std::streamsize)bb.length());
    if (os.bad()) {
        derror("write file %s failed", tmp_file.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }
    os.close();

    if (!utils::filesystem::rename_path(tmp_file, ffile)) {
        derror("move file from %s to %s failed", tmp_file.c_str(), ffile.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    return ERR_OK;
}

std::string replica_init_info::to_string()
{
    std::ostringstream oss;
    oss << "init_ballot = " << init_ballot << ", init_durable_decree = " << init_durable_decree
        << ", init_offset_in_shared_log = " << init_offset_in_shared_log
        << ", init_offset_in_private_log = " << init_offset_in_private_log;
    return oss.str();
}

error_code replica_app_info::load(const char *file)
{
    std::ifstream is(file, std::ios::binary);
    if (!is.is_open()) {
        derror("open file %s failed", file);
        return ERR_FILE_OPERATION_FAILED;
    }

    int64_t sz = 0;
    if (!::dsn::utils::filesystem::file_size(std::string(file), sz)) {
        derror("get file size of %s failed", file);
        return ERR_FILE_OPERATION_FAILED;
    }

    std::shared_ptr<char> buffer(dsn::utils::make_shared_array<char>(sz));
    is.read((char *)buffer.get(), sz);
    is.close();

    binary_reader reader(blob(buffer, sz));
    int magic;
    unmarshall(reader, magic, DSF_THRIFT_BINARY);

    if (magic != 0xdeadbeef) {
        derror("data in file %s is invalid (magic)", file);
        return ERR_INVALID_DATA;
    }

    unmarshall(reader, *_app, DSF_THRIFT_JSON);
    return ERR_OK;
}

error_code replica_app_info::store(const char *file)
{
    binary_writer writer;
    int magic = 0xdeadbeef;

    marshall(writer, magic, DSF_THRIFT_BINARY);
    // do not persistent envs to app info file
    if (_app->envs.empty()) {
        marshall(writer, *_app, DSF_THRIFT_JSON);
    } else {
        dsn::app_info tmp = *_app;
        tmp.envs.clear();
        marshall(writer, tmp, DSF_THRIFT_JSON);
    }

    std::string ffile = std::string(file);
    std::string tmp_file = ffile + ".tmp";

    std::ofstream os(tmp_file.c_str(),
                     (std::ofstream::out | std::ios::binary | std::ofstream::trunc));
    if (!os.is_open()) {
        derror("open file %s failed", tmp_file.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    auto data = writer.get_buffer();
    os.write((const char *)data.data(), (std::streamsize)data.length());
    os.close();

    if (!utils::filesystem::rename_path(tmp_file, ffile)) {
        derror("move file from %s to %s failed", tmp_file.c_str(), ffile.c_str());
        return ERR_FILE_OPERATION_FAILED;
    } else {
        return ERR_OK;
    }
}

/*static*/
void replication_app_base::register_storage_engine(const std::string &name, factory f)
{
    dsn::utils::factory_store<replication_app_base>::register_factory(
        name.c_str(), f, PROVIDER_TYPE_MAIN);
}
/*static*/
replication_app_base *replication_app_base::new_storage_instance(const std::string &name,
                                                                 replica *r)
{
    return dsn::utils::factory_store<replication_app_base>::create(
        name.c_str(), PROVIDER_TYPE_MAIN, r);
}

replication_app_base::replication_app_base(replica *replica)
    : replica_base(replica->get_gpid(), replica->name())
{
    _dir_data = utils::filesystem::path_combine(replica->dir(), "data");
    _dir_learn = utils::filesystem::path_combine(replica->dir(), "learn");
    _dir_backup = utils::filesystem::path_combine(replica->dir(), "backup");
    _last_committed_decree = 0;
    _replica = replica;

    install_perf_counters();
}

bool replication_app_base::is_primary() const
{
    return _replica->status() == partition_status::PS_PRIMARY;
}

void replication_app_base::install_perf_counters()
{
    // TODO: add custom perfcounters for replication_app_base
}

void replication_app_base::reset_counters_after_learning()
{
    // TODO: add custom perfcounters for replication_app_base
}

error_code replication_app_base::open_internal(replica *r)
{
    if (!dsn::utils::filesystem::directory_exists(_dir_data)) {
        derror("%s: replica data dir %s does not exist", r->name(), _dir_data.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    auto err = open();
    if (err == ERR_OK) {
        _last_committed_decree = last_durable_decree();

        err = _info.load(r->dir());
        if (err != ERR_OK) {
            derror("%s: load replica_init_info failed, err = %s", r->name(), err.to_string());
        }

        if (err == ERR_OK && last_durable_decree() < _info.init_durable_decree) {
            derror("%s: replica data is not complete coz last_durable_decree(%" PRId64
                   ") < init_durable_decree(%" PRId64 ")",
                   r->name(),
                   last_durable_decree(),
                   _info.init_durable_decree);
            err = ERR_INCOMPLETE_DATA;
        }
    }

    if (err != ERR_OK) {
        derror("%s: open replica app return %s", r->name(), err.to_string());
    }

    return err;
}

error_code replication_app_base::open_new_internal(replica *r,
                                                   int64_t shared_log_start,
                                                   int64_t private_log_start)
{
    dsn::utils::filesystem::remove_path(_dir_data);
    dsn::utils::filesystem::create_directory(_dir_data);

    if (!dsn::utils::filesystem::directory_exists(_dir_data)) {
        derror("%s: create replica data dir %s failed", r->name(), _dir_data.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    auto err = open();
    if (err == ERR_OK) {
        _last_committed_decree = last_durable_decree();
        err = update_init_info(_replica, shared_log_start, private_log_start, 0);
    }

    if (err != ERR_OK) {
        derror("%s: open replica app return %s", r->name(), err.to_string());
    }

    return err;
}

::dsn::error_code replication_app_base::open()
{
    const dsn::app_info *info = _replica->get_app_info();
    int argc = 1;
    argc += (2 * info->envs.size());
    // check whether replica have some extra envs that meta don't known
    const std::map<std::string, std::string> &extra_envs = _replica->get_replica_extra_envs();
    argc += (2 * extra_envs.size());

    std::unique_ptr<char *[]> argvs = make_unique<char *[]>(argc);
    char **argv = argvs.get();
    dassert(argv != nullptr, "");
    int idx = 0;
    argv[idx++] = (char *)(info->app_name.c_str());
    if (argc > 1) {
        for (auto &kv : info->envs) {
            argv[idx++] = (char *)(kv.first.c_str());
            argv[idx++] = (char *)(kv.second.c_str());
        }

        // combine extra envs
        for (auto &kv : extra_envs) {
            argv[idx++] = (char *)(kv.first.c_str());
            argv[idx++] = (char *)(kv.second.c_str());
        }
    }
    dassert(argc == idx, "%d VS %d", argc, idx);

    return start(argc, argv);
}

::dsn::error_code replication_app_base::close(bool clear_state)
{
    auto err = stop(clear_state);
    if (err == dsn::ERR_OK) {
        _last_committed_decree.store(0);
    } else {
        dwarn("%s: stop storage failed, err=%s", replica_name(), err.to_string());
    }
    return err;
}

::dsn::error_code replication_app_base::apply_checkpoint(chkpt_apply_mode mode,
                                                         const learn_state &state)
{
    int64_t current_commit_decree = last_committed_decree();
    dsn::error_code err = storage_apply_checkpoint(mode, state);
    if (ERR_OK == err && state.to_decree_included > current_commit_decree) {
        _last_committed_decree.store(state.to_decree_included);
    }
    return err;
}

int replication_app_base::on_batched_write_requests(int64_t decree,
                                                    uint64_t timestamp,
                                                    dsn::message_ex **requests,
                                                    int request_length)
{
    int storage_error = 0;
    for (int i = 0; i < request_length; ++i) {
        int e = on_request(requests[i]);
        if (e != 0) {
            derror("%s: got storage error when handler request(%s)",
                   _replica->name(),
                   requests[i]->header->rpc_name);
            storage_error = e;
        }
    }
    return storage_error;
}

::dsn::error_code replication_app_base::apply_mutation(const mutation *mu)
{
    dassert(mu->data.header.decree == last_committed_decree() + 1,
            "invalid mutation decree, decree = %" PRId64 " VS %" PRId64 "",
            mu->data.header.decree,
            last_committed_decree() + 1);
    dassert(mu->data.updates.size() == mu->client_requests.size(),
            "invalid mutation size, %d VS %d",
            (int)mu->data.updates.size(),
            (int)mu->client_requests.size());
    dassert(mu->data.updates.size() > 0, "");

    int request_count = static_cast<int>(mu->client_requests.size());
    dsn::message_ex **batched_requests =
        (dsn::message_ex **)alloca(sizeof(dsn::message_ex *) * request_count);
    dsn::message_ex **faked_requests =
        (dsn::message_ex **)alloca(sizeof(dsn::message_ex *) * request_count);
    int batched_count = 0;
    int faked_count = 0;
    for (int i = 0; i < request_count; i++) {
        const mutation_update &update = mu->data.updates[i];
        dsn::message_ex *req = mu->client_requests[i];
        if (update.code != RPC_REPLICATION_WRITE_EMPTY) {
            dinfo("%s: mutation %s #%d: dispatch rpc call %s",
                  _replica->name(),
                  mu->name(),
                  i,
                  update.code.to_string());

            if (req == nullptr) {
                req = dsn::message_ex::create_received_request(
                    update.code,
                    (dsn_msg_serialize_format)update.serialization_type,
                    (void *)update.data.data(),
                    update.data.length());
                faked_requests[faked_count++] = req;
            }

            batched_requests[batched_count++] = req;
        } else {
            // empty mutation write
            dinfo("%s: mutation %s #%d: dispatch rpc call %s",
                  _replica->name(),
                  mu->name(),
                  i,
                  update.code.to_string());
        }
    }

    int perror = on_batched_write_requests(
        mu->data.header.decree, mu->data.header.timestamp, batched_requests, batched_count);

    // release faked requests
    for (int i = 0; i < faked_count; i++) {
        faked_requests[i]->release_ref();
    }

    if (perror != 0) {
        derror("%s: mutation %s: get internal error %d", _replica->name(), mu->name(), perror);
        return ERR_LOCAL_APP_FAILURE;
    }

    ++_last_committed_decree;

    if (_replica->verbose_commit_log()) {
        auto status = _replica->status();
        const char *str;
        switch (status) {
        case partition_status::PS_INACTIVE:
            str = "I";
            break;
        case partition_status::PS_PRIMARY:
            str = "P";
            break;
        case partition_status::PS_SECONDARY:
            str = "S";
            break;
        case partition_status::PS_POTENTIAL_SECONDARY:
            str = "PS";
            break;
        default:
            dassert(false, "status = %s", enum_to_string(status));
            __builtin_unreachable();
        }
        ddebug("%s: mutation %s committed on %s, batched_count = %d",
               _replica->name(),
               mu->name(),
               str,
               batched_count);
    }

    _replica->update_commit_statistics(1);

    return ERR_OK;
}

::dsn::error_code replication_app_base::update_init_info(replica *r,
                                                         int64_t shared_log_offset,
                                                         int64_t private_log_offset,
                                                         int64_t durable_decree)
{
    _info.crc = 0;
    _info.magic = 0xdeadbeef;
    _info.init_ballot = r->get_ballot();
    _info.init_durable_decree = durable_decree;
    _info.init_offset_in_shared_log = shared_log_offset;
    _info.init_offset_in_private_log = private_log_offset;

    error_code err = _info.store(r->dir());
    if (err != ERR_OK) {
        derror("%s: store replica_init_info failed, err = %s", r->name(), err.to_string());
    }

    return err;
}

::dsn::error_code replication_app_base::update_init_info_ballot_and_decree(replica *r)
{
    return update_init_info(r,
                            _info.init_offset_in_shared_log,
                            _info.init_offset_in_private_log,
                            r->last_durable_decree());
}
}
} // end namespace
