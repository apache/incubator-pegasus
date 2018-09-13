set(MY_PROJ_NAME dsn_meta_server)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB_RECURSE")

set(MY_PROJ_INC_PATH)

set(MY_PROJ_LIBS
    dsn_replication_common
    dsn.block_service.local
    dsn.block_service.fds
    dsn.failure_detector
    dsn.replication.zookeeper_provider
    dsn_cli
    zookeeper_mt
    galaxy-fds-sdk-cpp
    PocoNet
    PocoFoundation
    PocoNetSSL
    PocoJSON
    crypto
    )

set(MY_PROJ_LIB_PATH
    ${LIBFDS_LIB_DIR}
    ${POCO_LIB_DIR}
)

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_shared_library()
