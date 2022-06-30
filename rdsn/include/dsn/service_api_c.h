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
 *
 * ------------------------------------------------------------------------------
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version in cpp
 *     July, 2015, @imzhenyu (Zhenyu Guo), refactor and refined in c
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), decompose into several files for V1 release
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

// common data structures and macros
#include <dsn/c/api_common.h>

// rDSN uses event-driven programming model, and
// this file defines the task(i.e., event) abstraction and related
#include <dsn/c/api_task.h>

// service API for app/framework development,
// including threading/tasking, thread synchronization,
// RPC, asynchronous file IO, environment, etc.
#include <dsn/c/api_layer1.h>

// application/framework model in rDSN
#include <dsn/c/app_model.h>

// some useful utility functions provided by rDSN,
// such as logging, performance counter, checksum,
// command line interface registration and invocation,
// etc.
#include <dsn/c/api_utilities.h>
