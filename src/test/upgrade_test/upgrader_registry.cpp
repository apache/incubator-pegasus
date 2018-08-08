#include "upgrader_registry.h"
#include "upgrader_handler.h"
#include "upgrader_handler_shell.h"

using namespace pegasus::test;

void register_upgrade_handlers()
{
    upgrader_handler::register_factory<upgrader_handler_shell>("shell");
}
