#include "killer_registry.h"
#include "killer_handler.h"
#include "killer_handler_shell.h"

using namespace pegasus::test;

void register_kill_handlers() { killer_handler::register_factory<killer_handler_shell>("shell"); }
