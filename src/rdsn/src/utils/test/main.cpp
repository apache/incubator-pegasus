// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>
#include <dsn/c/api_utilities.h>
#include <dsn/tool-api/logging_provider.h>
#include <dsn/utility/flags.h>

extern void command_manager_module_init();

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    command_manager_module_init();
    // init logging
    dsn_log_init("dsn::tools::simple_logger", "./", nullptr);

    dsn::flags_initialize();

    return RUN_ALL_TESTS();
}
