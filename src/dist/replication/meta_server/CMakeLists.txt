set(MY_PROJ_NAME dsn_meta_server)

set(DUPLICATION_SRC
        duplication/duplication_info.cpp
        duplication/meta_duplication_service.cpp
        )

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "${DUPLICATION_SRC}")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB_RECURSE")

set(MY_PROJ_LIBS
    dsn_replication_common
    dsn.block_service
    dsn.block_service.local
    dsn.block_service.fds
    dsn.failure_detector
    dsn.replication.zookeeper_provider
    dsn_dist_cmd
    dsn_http
    dsn_runtime
    dsn_aio
    zookeeper_mt
    galaxy-fds-sdk-cpp
    PocoNet
    PocoFoundation
    PocoNetSSL
    PocoJSON
    crypto
    )

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_shared_library()
