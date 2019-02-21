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
