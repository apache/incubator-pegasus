// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#define RETRY_OPERATION(CLIENT_FUNCTION, RESULT)                                                   \
    do {                                                                                           \
        for (int i = 0; i < 60; ++i) {                                                             \
            RESULT = CLIENT_FUNCTION;                                                              \
            if (RESULT == 0) {                                                                     \
                break;                                                                             \
            } else {                                                                               \
                std::this_thread::sleep_for(std::chrono::milliseconds(500));                       \
            }                                                                                      \
        }                                                                                          \
    } while (0)
