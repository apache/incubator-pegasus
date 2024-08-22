# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set(MY_PROJ_NAME "pegasus_replica_server")

set(MY_PROJ_SRC ${REPLICA_SERVER_SRC})
set(MY_SRC_SEARCH_MODE "GLOB")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/../")
set(MY_PROJ_LIBS ${REPLICA_SERVER_LIBS})
set(MY_BOOST_LIBS Boost::system Boost::filesystem)
SET(CMAKE_INSTALL_RPATH ".")
SET(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

dsn_add_executable()
dsn_install_executable()
