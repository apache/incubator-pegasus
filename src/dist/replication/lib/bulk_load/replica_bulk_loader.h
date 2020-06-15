// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dist/replication/lib/replica.h>
#include <dist/replication/lib/replica_context.h>
#include <dist/replication/lib/replica_stub.h>

namespace dsn {
namespace replication {

class replica_bulk_loader : replica_base
{
public:
    explicit replica_bulk_loader(replica *r);
    ~replica_bulk_loader();

    void on_bulk_load(const bulk_load_request &request, /*out*/ bulk_load_response &response);

    void on_group_bulk_load(const group_bulk_load_request &request,
                            /*out*/ group_bulk_load_response &response);

private:
    void broadcast_group_bulk_load(const bulk_load_request &meta_req);
    void on_group_bulk_load_reply(error_code err,
                                  const group_bulk_load_request &req,
                                  const group_bulk_load_response &resp);

    error_code do_bulk_load(const std::string &app_name,
                            bulk_load_status::type meta_status,
                            const std::string &cluster_name,
                            const std::string &provider_name);

    // compare meta bulk load status and local bulk load status
    // \return ERR_INVALID_STATE if local status is invalid
    // for example, if meta status is ingestion, replica local status can only be downloaded or
    // ingestion, if local status is other status, will return ERR_INVALID_STATE
    error_code validate_bulk_load_status(bulk_load_status::type meta_status,
                                         bulk_load_status::type local_status);

    // replica start or restart download sst files from remote provider
    // \return ERR_BUSY if node has already had enough replica executing downloading
    // \return download errors by function `download_sst_files`
    error_code start_download(const std::string &app_name,
                              const std::string &cluster_name,
                              const std::string &provider_name);

    // download metadata and sst files from remote provider
    // metadata and sst files will be downloaded in {_dir}/.bulk_load directory
    // \return ERR_FILE_OPERATION_FAILED: create local bulk load dir failed
    // \return download metadata file error, see function `do_download`
    // \return parse metadata file error, see function `parse_bulk_load_metadata`
    error_code download_sst_files(const std::string &app_name,
                                  const std::string &cluster_name,
                                  const std::string &provider_name);

    // \return ERR_FILE_OPERATION_FAILED: file not exist, get size failed, open file failed
    // \return ERR_CORRUPTION: parse failed
    error_code parse_bulk_load_metadata(const std::string &fname);

    // update download progress after downloading sst files succeed
    void update_bulk_load_download_progress(uint64_t file_size, const std::string &file_name);

    void try_decrease_bulk_load_download_count();
    void check_download_finish();
    void start_ingestion();
    void check_ingestion_finish();

    void cleanup_download_task();
    void clear_bulk_load_states();

    void report_bulk_load_states_to_meta(bulk_load_status::type remote_status,
                                         bool report_metadata,
                                         /*out*/ bulk_load_response &response);
    void report_group_download_progress(/*out*/ bulk_load_response &response);
    void report_group_ingestion_status(/*out*/ bulk_load_response &response);

    void report_bulk_load_states_to_primary(bulk_load_status::type remote_status,
                                            /*out*/ group_bulk_load_response &response);

    ///
    /// bulk load path on remote file provider:
    /// <bulk_load_root>/<cluster_name>/<app_name>/{bulk_load_info}
    /// <bulk_load_root>/<cluster_name>/<app_name>/<partition_index>/<file_name>
    /// <bulk_load_root>/<cluster_name>/<app_name>/<partition_index>/bulk_load_metadata
    ///
    // get partition's file dir on remote file provider
    inline std::string get_remote_bulk_load_dir(const std::string &app_name,
                                                const std::string &cluster_name,
                                                uint32_t pidx) const
    {
        std::ostringstream oss;
        oss << _replica->_options->bulk_load_provider_root << "/" << cluster_name << "/" << app_name
            << "/" << pidx;
        return oss.str();
    }

    inline bulk_load_status::type get_bulk_load_status() const { return _status; }

    inline void set_bulk_load_status(bulk_load_status::type status) { _status = status; }

    //
    // helper functions
    //
    partition_status::type status() const { return _replica->status(); }
    ballot get_ballot() const { return _replica->get_ballot(); }
    task_tracker *tracker() { return _replica->tracker(); }

private:
    replica *_replica;
    replica_stub *_stub;

    friend class replica;
    friend class replica_bulk_loader_test;

    bulk_load_status::type _status{bulk_load_status::BLS_INVALID};
    bulk_load_metadata _metadata;
    std::atomic<uint64_t> _cur_downloaded_size{0};
    std::atomic<int32_t> _download_progress{0};
    std::atomic<error_code> _download_status{ERR_OK};
    // file_name -> downloading task
    std::map<std::string, task_ptr> _download_task;
};

} // namespace replication
} // namespace dsn
