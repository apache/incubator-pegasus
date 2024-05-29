/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <iostream>
#include <string>

#include "pegasus/git_commit.h"
#include "pegasus/version.h"
#include "runtime/app_model.h"
#include "server/server_utils.h"
#include "shell/command_executor.h"
#include "shell/commands.h"

bool version(command_executor *e, shell_context *sc, arguments args)
{
    std::ostringstream oss;
    oss << "Pegasus Shell " << PEGASUS_VERSION << " (" << PEGASUS_GIT_COMMIT << ") "
        << PEGASUS_BUILD_TYPE;
    std::cout << oss.str() << std::endl;
    return true;
}

bool exit_shell(command_executor *e, shell_context *sc, arguments args)
{
    dsn_exit(0);
    return true;
}
