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

set(MY_PROJ_NAME "security_test")
set(MY_PROJ_SRC "")

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS
        absl::strings
        dsn_http
        curl
        dsn_meta_server
        dsn_utils
        dsn_security
        dsn_ranger
        dsn_replica_server
        sasl2
        gssapi_krb5
        krb5
        gtest
        function_test_utils
        rocksdb
        test_utils)

set(MY_BOOST_LIBS Boost::system Boost::filesystem)

set(MY_BINPLACES
        ./config.ini
        ../run.sh)

dsn_add_test()
