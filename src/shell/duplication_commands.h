// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "command_executor.h"

extern bool add_dup(command_executor *e, shell_context *sc, arguments args);

extern bool query_dup(command_executor *e, shell_context *sc, arguments args);

extern bool remove_dup(command_executor *e, shell_context *sc, arguments args);

extern bool start_dup(command_executor *e, shell_context *sc, arguments args);

extern bool pause_dup(command_executor *e, shell_context *sc, arguments args);
