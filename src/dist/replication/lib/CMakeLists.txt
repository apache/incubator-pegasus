set(MY_PROJ_NAME dsn_replica_server)

set(DUPLICATION_SRC
        duplication/replica_duplicator_manager.cpp
        duplication/duplication_sync_timer.cpp
        duplication/replica_duplicator.cpp
        duplication/duplication_pipeline.cpp
        duplication/load_from_private_log.cpp
        duplication/mutation_batch.cpp
)

set(BACKUP_SRC
        backup/replica_backup_manager.cpp
)

set(BULK_LOAD_SRC
    bulk_load/replica_bulk_loader.cpp)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC
        ${DUPLICATION_SRC}
        ${BACKUP_SRC}
        ${BULK_LOAD_SRC}
)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS 
    dsn_replication_common
    dsn.failure_detector
    dsn.failure_detector.multimaster
    dsn.block_service
    dsn_nfs
    dsn_dist_cmd
    dsn_http
    dsn_runtime
    dsn_aio
    )

set(MY_BOOST_LIBS Boost::regex)

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_shared_library()

add_subdirectory(duplication/test)

add_subdirectory(backup/test)

add_subdirectory(bulk_load/test)
