set(MY_PROJ_NAME dsn_replica_server)

set(DUPLICATION_SRC
        duplication/duplication_sync_timer.cpp
)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC ${DUPLICATION_SRC})

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "")

set(MY_PROJ_LIBS 
    dsn_replication_common
    dsn.failure_detector
    dsn.failure_detector.multimaster
    dsn_nfs
    dsn_cli
    dsn_http
    )

set(MY_PROJ_LIB_PATH "")

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_shared_library()
