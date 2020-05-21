// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "replica.h"

#include <fstream>

#include <dsn/dist/block_service.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/filesystem.h>

namespace dsn {
namespace replication {

// ThreadPool: THREAD_POOL_REPLICATION, THREAD_POOL_REPLICATION_LONG
error_code replica::do_download(const std::string &remote_dir,
                                const std::string &local_dir,
                                const std::string &file_name,
                                dist::block_service::block_filesystem *fs,
                                /*out*/ uint64_t &download_file_size)
{
    // TODO(heyuchen): TBD
    // download files from remote provider
    // this function can also be used in restore
    return ERR_OK;
}

} // namespace replication
} // namespace dsn
