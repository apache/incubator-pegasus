/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     this file define the C Service API in rDSN
 *
 * ------------------------------------------------------------------------------
 *
 *  The service system call API for Zion
 * -------------------------------------------
 *  Summary:
 *  (1) rich API for common distributed system development
 *      - thread pools and tasking
 *      - thread synchronization
 *      - remote procedure calls
 *      - asynchnous file operations
 *      - envrionment inputs
 *      - rDSN app model, system and other utilities
 *  (2) portable
 *      - compilable on many platforms (currently linux, windows, FreeBSD, MacOS)
 *      - system calls are in C so that later language wrappers are possibles.
 *  (3) high performance
 *      - all low level components can be plugged with the tool API (in C++)
 *        besides the existing high performance providers;
 *      - developers can also configure thread pools, thread numbers, thread/task
 *        priorities, CPU core affinities, throttling policies etc. declaratively
 *        to build a best threading model for upper apps.
 *  (4) ease of intergration
 *      - support many languages through language wrappers based on this c interface
 *      - easy support for existing protocols (thrift/protobuf etc.)
 *      - integrate with existing platform infra with low level providers (plug-in),
 *        such as loggers, performance counters, etc.
 *  (5) rich debug, development tools and runtime policies support
 *      - tool API with task granularity semantic for further tool and runtime policy development.
 *      - rich existing tools, tracer, profiler, simulator, model checker, replayer, global checker
 *  (7) PRINCIPLE: all non-determinims must be go through these system calls so that powerful
 *      internal tools are possible - replay, model checking, replication, ...,
 *      AND, it is still OK to call other DETERMINISTIC APIs for applications.
 */

// common data structures and macros

#pragma once

#include <stdint.h>
#include <stddef.h>
#include <stdarg.h>
#include "utils/dlib.h"

#ifdef __cplusplus
#define DEFAULT(value) = value
#define NORETURN [[noreturn]]
#else
#define DEFAULT(value)
#define NORETURN
#include <stdbool.h>
#endif

#define DSN_MAX_TASK_CODE_NAME_LENGTH 48
#define DSN_MAX_ERROR_CODE_NAME_LENGTH 48
#define DSN_MAX_ADDRESS_NAME_LENGTH 48
#define DSN_MAX_BUFFER_COUNT_IN_MESSAGE 64
#define DSN_MAX_APP_TYPE_NAME_LENGTH 32
#define DSN_MAX_CALLBAC_COUNT 32
#define DSN_MAX_APP_COUNT_IN_SAME_PROCESS 256
#define DSN_MAX_PATH 1024
