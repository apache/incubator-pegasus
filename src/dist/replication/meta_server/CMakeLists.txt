add_definitions(-DDSN_META_SERVER_DYNAMIC_LIB)

set(MY_PROJ_NAME dsn.meta_server)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH ../client_lib ../../failure_detector)

set(MY_PROJ_LIBS
    dsn.replication.clientlib
    dsn.failure_detector
    dsn.replication.zookeeper_provider
    zookeeper_mt
    )

set(MY_PROJ_LIB_PATH "")

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_shared_library()
