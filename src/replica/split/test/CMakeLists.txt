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

set(MY_PROJ_NAME dsn_replica_split_test)

set(MY_PROJ_SRC "")

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS dsn_meta_server
        dsn_replica_server
        dsn_replication_common
        dsn_runtime
        hashtable
        gtest
)

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

set(MY_BINPLACES
        config-test.ini
        run.sh
)

dsn_add_test()
