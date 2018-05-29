// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "killer_registry.h"
#include "killer_handler.h"
#include "killer_handler_shell.h"

using namespace pegasus::test;

void register_kill_handlers() { killer_handler::register_factory<killer_handler_shell>("shell"); }
