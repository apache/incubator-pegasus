set(MY_PROJ_NAME dsn.replication.simple_kv.module)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
ms_find_source_files("CXX" "${CMAKE_CURRENT_SOURCE_DIR}/../exe" "GLOB" MY_PROJ_SRC)

add_definitions(-DDSN_RUN_USE_SVCHOST)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../exe")

set(MY_BOOST_PACKAGES system)

set(MY_PROJ_LIBS dsn.replication.global_checker dsn.replication dsn.replication.meta_server dsn.replication.clientlib dsn.failure_detector
dsn.replication.zookeeper_provider zookeeper_mt)

set(MY_PROJ_LIB_PATH "${ZOOKEEPER_LIB_DIR}")

set(INI_FILES "")
file(GLOB
    INI_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.ini"
    )

# Extra files that will be installed
set(MY_BINPLACES ${INI_FILES})

dsn_add_shared_library()
