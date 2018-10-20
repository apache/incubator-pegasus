#pragma once

#include <dsn/utility/singleton_store.h>
#include <dsn/tool-api/zlocks.h>

namespace dsn {
namespace dist {
namespace block_service {
class block_filesystem;
}
}
}

namespace dsn {
namespace replication {

// a singleton for rDSN service_engine to register all blocks, this should be called only once
class block_service_registry : public utils::singleton<block_service_registry>
{
public:
    block_service_registry();
};

// this should be shared within a service node
// we can't make the block_service_manager shared among service nodes because of rDSN's
// share-nothing archiecture among different apps
class block_service_manager
{
public:
    block_service_manager();
    dist::block_service::block_filesystem *get_block_filesystem(const std::string &provider);

private:
    block_service_registry &_registry_holder;

    mutable zrwlock_nr _fs_lock;
    std::map<std::string, std::unique_ptr<dist::block_service::block_filesystem>> _fs_map;
};
}
}
