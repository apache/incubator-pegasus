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

set(MY_PROJ_NAME dsn_meta_server)

set(DUPLICATION_SRC
        duplication/duplication_info.cpp
        duplication/meta_duplication_service.cpp
        )

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "${DUPLICATION_SRC}")

set(MY_PROJ_LIBS
    dsn_replication_common
    dsn.block_service
    dsn.block_service.local
    dsn.block_service.fds
    dsn.block_service.hdfs
    dsn.failure_detector
    dsn.replication.zookeeper_provider
    dsn_dist_cmd
    dsn_http
    dsn_runtime
    dsn_aio
    zookeeper
    galaxy-fds-sdk-cpp
    PocoNet
    PocoFoundation
    PocoNetSSL
    PocoJSON
    crypto
    hashtable
    hdfs
    )

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

# Extra files that will be installed
set(MY_BINPLACES "")

add_definitions(-DDSN_MOCK_TEST)

dsn_add_shared_library()

add_subdirectory(test)
