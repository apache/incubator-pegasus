set(MY_PROJ_NAME dsn.tools.repli)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "")

set(MY_BOOST_PACKAGES system filesystem)

set(MY_PROJ_LIB_PATH "")
set(MY_PROJ_LIBS dsn_layer2_stateful_type1
                 dsn.replication.clientlib
                 dsn.block_service.local
                 dsn.block_service.fds
                 dsn.failure_detector
                 dsn.failure_detector.multimaster
                 galaxy-fds-sdk-cpp
                 PocoNet
                 PocoFoundation
                 PocoNetSSL
                 PocoJSON
                 crypto
                )

# Extra files that will be installed
set(MY_BINPLACES "${CMAKE_CURRENT_SOURCE_DIR}/config.ini")

dsn_add_executable()
