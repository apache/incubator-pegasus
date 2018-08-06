#include <dsn/utility/smart_pointers.h>
#include <dsn/tool-api/async_calls.h>
#include <dsn/dist/nfs_node.h>

#include "nfs_node_simple.h"

namespace dsn {

std::unique_ptr<nfs_node> nfs_node::create()
{
    return dsn::make_unique<dsn::service::nfs_node_simple>();
}

aio_task_ptr nfs_node::copy_remote_directory(rpc_address remote,
                                             const std::string &source_dir,
                                             const std::string &dest_dir,
                                             bool overwrite,
                                             bool high_priority,
                                             task_code callback_code,
                                             task_tracker *tracker,
                                             aio_handler &&callback,
                                             int hash)
{
    return copy_remote_files(remote,
                             source_dir,
                             {},
                             dest_dir,
                             overwrite,
                             high_priority,
                             callback_code,
                             tracker,
                             std::move(callback),
                             hash);
}

aio_task_ptr nfs_node::copy_remote_files(rpc_address remote,
                                         const std::string &source_dir,
                                         const std::vector<std::string> &files,
                                         const std::string &dest_dir,
                                         bool overwrite,
                                         bool high_priority,
                                         task_code callback_code,
                                         task_tracker *tracker,
                                         aio_handler &&callback,
                                         int hash)
{
    auto cb = dsn::file::create_aio_task(callback_code, tracker, std::move(callback), hash);

    std::shared_ptr<remote_copy_request> rci = std::make_shared<remote_copy_request>();
    rci->source = remote;
    rci->source_dir = source_dir;
    rci->files = files;
    rci->dest_dir = dest_dir;
    rci->overwrite = overwrite;
    rci->high_priority = high_priority;
    call(rci, cb);

    return cb;
}
}
