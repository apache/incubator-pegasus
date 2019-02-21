// Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

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
