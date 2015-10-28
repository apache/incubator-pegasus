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

# include "replica.h"
# include "mutation.h"
# include <dsn/internal/factory_store.h>
# include "mutation_log.h"
# include <fstream>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.2pc"

namespace dsn { namespace replication {

void register_replica_provider(replica_app_factory f, const char* name)
{
    ::dsn::utils::factory_store<replication_app_base>::register_factory(name, f, PROVIDER_TYPE_MAIN);
}

error_code replication_app_info::load(const char* file)
{
    std::ifstream is(file, std::ios::binary);
    if (!is.is_open())
    {
        derror("open file %s failed", file);
        return ERR_FILE_OPERATION_FAILED;
    }

    magic = 0x0;
    is.read((char*)this, sizeof(*this));
    is.close();

    if (magic != 0xdeadbeef)
    {
        derror("data in file %s is invalid (magic)", file);
        return ERR_INVALID_DATA;
    }

    auto fcrc = crc;
    crc = 0; // set for crc computation
    auto lcrc = dsn_crc32_compute((const void*)this, sizeof(*this), 0);
    crc = fcrc; // recover

    if (lcrc != fcrc)
    {
        derror("data in file %s is invalid (crc)", file);
        return ERR_INVALID_DATA;
    }
    
    return ERR_OK;
}

error_code replication_app_info::store(const char* file)
{
    std::string ffile = std::string(file);
    std::string tmp_file = ffile + ".tmp";

    std::ofstream os(tmp_file.c_str(), (std::ofstream::out | std::ios::binary | std::ofstream::trunc));
    if (!os.is_open())
    {
        derror("open file %s failed", tmp_file.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    // compute crc
    crc = 0;
    crc = dsn_crc32_compute((const void*)this, sizeof(*this), 0);

    os.write((const char*)this, sizeof(*this));
    os.close();

    if (!utils::filesystem::rename_path(tmp_file, ffile, true))
    {
        return ERR_FILE_OPERATION_FAILED;
    }

    dinfo("update app init info in %s, ballot = %lld, decree = %lld, shared_log_offset = %lld",
        ffile.c_str(),
        init_ballot,
        init_decree,
        init_offset_in_shared_log
        );
    return ERR_OK;
}

replication_app_base::replication_app_base(replica* replica)
{
    _physical_error = 0;
    _dir_data = replica->dir() + "/data";
    _dir_learn = replica->dir() + "/learn";
    _is_delta_state_learning_supported = false;

    _replica = replica;
    _last_committed_decree = _last_durable_decree = 0;

	if (!dsn::utils::filesystem::create_directory(_dir_data))
	{
		dassert(false, "Fail to create directory %s.", _dir_data.c_str());
	}

	if (!dsn::utils::filesystem::create_directory(_dir_learn))
	{
		dassert(false, "Fail to create directory %s.", _dir_learn.c_str());
	}
}

const char* replication_app_base::replica_name() const
{
    return _replica->name();
}

error_code replication_app_base::open_internal(replica* r, bool create_new)
{
    auto err = open(create_new);
    if (err == 0)
    {
        if (!create_new)
        {
            std::string info_path = utils::filesystem::path_combine(r->dir(), ".info");
            err = _info.load(info_path.c_str());
        }
    }
    return err == 0 ? ERR_OK : ERR_LOCAL_APP_FAILURE;
}

error_code replication_app_base::write_internal(mutation_ptr& mu)
{
    dassert (mu->data.header.decree == last_committed_decree() + 1, "");

    if (mu->rpc_code != RPC_REPLICATION_WRITE_EMPTY)
    {
        binary_reader reader(mu->data.updates[0]);
        dsn_message_t resp = (mu->client_msg() ? dsn_msg_create_response(mu->client_msg()) : nullptr);
        dispatch_rpc_call(mu->rpc_code, reader, resp);
    }
    else
    {
        on_empty_write();
    }

    if (_physical_error != 0)
    {
        derror("physical error %d occurs in replication local app %s", _physical_error, data_dir().c_str());
    }

    return _physical_error == 0 ? ERR_OK : ERR_LOCAL_APP_FAILURE;
}

error_code replication_app_base::save_init_info(replica* r, int64_t shared_log_offset, int64_t private_log_offset)
{
    _info.crc = 0;
    _info.magic = 0xdeadbeef;
    _info.init_ballot = r->get_ballot();
    _info.init_decree = r->last_committed_decree();
    _info.init_offset_in_shared_log = shared_log_offset;
    _info.init_offset_in_shared_log = private_log_offset;

    std::string info_path = utils::filesystem::path_combine(r->dir(), ".info");
    return _info.store(info_path.c_str());
}

void replication_app_base::dispatch_rpc_call(int code, binary_reader& reader, dsn_message_t response)
{
    auto it = _handlers.find(code);
    if (it != _handlers.end())
    {
        if (response)
        {
            int err = 0; // replication layer error
            ::marshall(response, err);            
        }
        it->second(reader, response);
    }
    else
    {
        dassert(false, "cannot find handler for rpc code %d in %s", code, data_dir().c_str());
    }
}

}} // end namespace
