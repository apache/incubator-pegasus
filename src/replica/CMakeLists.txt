# The MIT License (MIT)
#
# Copyright (c) 2015 Microsoft Corporation
#
# -=- Robust Distributed System Nucleus (rDSN) -=-
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

set(MY_PROJ_NAME dsn_replica_server)

set(DUPLICATION_SRC
        duplication/replica_duplicator_manager.cpp
        duplication/duplication_sync_timer.cpp
        duplication/replica_duplicator.cpp
        duplication/replica_follower.cpp
        duplication/duplication_pipeline.cpp
        duplication/load_from_private_log.cpp
        duplication/mutation_batch.cpp
)

set(BACKUP_SRC backup/replica_backup_manager.cpp
               backup/cold_backup_context.cpp
               backup/replica_backup_server.cpp
)

set(BULK_LOAD_SRC bulk_load/replica_bulk_loader.cpp)

set(SPLIT_SRC split/replica_split_manager.cpp)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC
        ${DUPLICATION_SRC}
        ${BACKUP_SRC}
        ${BULK_LOAD_SRC}
        ${SPLIT_SRC}
)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS
        absl::strings
        dsn_replication_common
        dsn.failure_detector
        dsn.block_service
        dsn.block_service.local
        dsn.block_service.hdfs
        dsn_nfs
        dsn_dist_cmd
        dsn_http
        curl
        dsn_runtime
        dsn_aio
        dsn_meta_server
        dsn_security
        rocksdb
        lz4
        zstd
        snappy)

set(MY_BOOST_LIBS Boost::filesystem)

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_shared_library()

add_subdirectory(duplication/test)
add_subdirectory(backup/test)
add_subdirectory(bulk_load/test)
add_subdirectory(split/test)
add_subdirectory(storage)
add_subdirectory(test)
