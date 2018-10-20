#include <dsn/utility/factory_store.h>
#include <dsn/dist/block_service.h>
#include "block_service_manager.h"
#include "../../block_service/fds/fds_service.h"
#include "../../block_service/local/local_service.h"

using namespace dsn::dist::block_service;

namespace dsn {
namespace replication {

block_service_registry::block_service_registry()
{
    bool ans;
    ans = utils::factory_store<dist::block_service::block_filesystem>::register_factory(
        "fds_service",
        dist::block_service::block_filesystem::create<dist::block_service::fds_service>,
        PROVIDER_TYPE_MAIN);
    dassert(ans, "register fds_service failed");

    ans = utils::factory_store<dist::block_service::block_filesystem>::register_factory(
        "local_service",
        dist::block_service::block_filesystem::create<dist::block_service::local_service>,
        PROVIDER_TYPE_MAIN);
    dassert(ans, "register local_service failed");
}

block_service_manager::block_service_manager()
    : // we got a instance of block_service_registry each time we create a block_service_manger
      // to make sure that the filesystem providers are registered
      _registry_holder(block_service_registry::instance())
{
}

block_filesystem *block_service_manager::get_block_filesystem(const std::string &provider)
{
    {
        zauto_read_lock l(_fs_lock);
        auto iter = _fs_map.find(provider);
        if (iter != _fs_map.end())
            return iter->second.get();
    }

    {
        zauto_write_lock l(_fs_lock);
        auto iter = _fs_map.find(provider);
        if (iter != _fs_map.end())
            return iter->second.get();

        const char *provider_type = dsn_config_get_value_string(
            (std::string("block_service.") + provider).c_str(), "type", "", "block service type");

        block_filesystem *fs =
            utils::factory_store<block_filesystem>::create(provider_type, PROVIDER_TYPE_MAIN);
        if (fs == nullptr) {
            derror("acquire block filesystem failed, provider = %s, provider_type = %s",
                   provider.c_str(),
                   provider_type);
            return nullptr;
        }

        const char *arguments =
            dsn_config_get_value_string((std::string("block_service.") + provider).c_str(),
                                        "args",
                                        "",
                                        "args for block_service");

        std::vector<std::string> args;
        utils::split_args(arguments, args);
        dsn::error_code err = fs->initialize(args);

        if (dsn::ERR_OK == err) {
            ddebug("create block filesystem ok for provider(%s)", provider.c_str());
            _fs_map.emplace(provider, std::unique_ptr<block_filesystem>(fs));
            return fs;
        } else {
            derror("create block file system err(%s) for provider(%s)",
                   err.to_string(),
                   provider.c_str());
            delete fs;
            return nullptr;
        }
    }
}
}
}
