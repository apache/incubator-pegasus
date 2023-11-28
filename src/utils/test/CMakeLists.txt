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

set(MY_PROJ_NAME dsn_utils_tests)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS dsn_http
                 dsn_runtime
                 dsn_utils
                 gtest
                 test_utils
                 rocksdb)

set(MY_BOOST_LIBS Boost::system Boost::filesystem)

# Extra files that will be installed
set(MY_BINPLACES "${CMAKE_CURRENT_SOURCE_DIR}/config-bad-section.ini"
        "${CMAKE_CURRENT_SOURCE_DIR}/config-dup-key.ini"
        "${CMAKE_CURRENT_SOURCE_DIR}/config-dup-section.ini"
        "${CMAKE_CURRENT_SOURCE_DIR}/config-empty.ini"
        "${CMAKE_CURRENT_SOURCE_DIR}/config-no-key.ini"
        "${CMAKE_CURRENT_SOURCE_DIR}/config-no-section.ini"
        "${CMAKE_CURRENT_SOURCE_DIR}/config-null-section.ini"
        "${CMAKE_CURRENT_SOURCE_DIR}/config-sample.ini"
        "${CMAKE_CURRENT_SOURCE_DIR}/config-unmatch-section.ini"
        "${CMAKE_CURRENT_SOURCE_DIR}/run.sh"
        "${CMAKE_CURRENT_SOURCE_DIR}/clear.sh"
        )
add_subdirectory(nth_element_bench)
add_definitions(-Wno-dangling-else)
dsn_add_test()
