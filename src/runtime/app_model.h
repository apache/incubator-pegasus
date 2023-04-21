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

// application/framework model in rDSN

#pragma once

/*!
 mimic an app as if the following execution in the current thread are
 executed in the target app's threads.

 \param app_name name of the application, note it is not the type name
 \param index    one-based index of the application instances

 \return true if it succeeds, false if it fails.

 This is useful when we want to leverage 3rd party library into rDSN
 application and call rDSN service API in the threads that are created
 by the 3rd party code.

 For cases we simply want to use a rDSN-based client library in a non-rDSN
 application, developers can simply set [core] enable_default_app_mimic = true
 in configuration file. See more details at \ref enable_default_app_mimic.

 */
extern bool dsn_mimic_app(const char *app_role, int index);

/*!
 start the system with given configuration

 \param config           the configuration file for this run
 \param is_server whether it is server or not, default is false

 \return true if it succeeds, false if it fails.
 */
extern bool dsn_run_config(const char *config, bool is_server = false);

/*!
 start the system with given arguments

 \param argc             argc in C main convention
 \param argv             argv in C main convention
 \param is_server whether it is server or not, default is false

 \return true if it succeeds, false if it fails.

 Usage:
   config-file [-cargs k1=v1;k2=v2] [-app_list app_name1@index1;app_name2@index]

 Examples:
 - config.ini -app_list replica@1 to start the first replica as a new process
 - config.ini -app_list replica to start ALL replicas (count specified in config) as a new
 process
 - config.ini -app_list replica -cargs replica-port=34556 to start ALL replicas
   with given port variable specified in config.ini
 - config.ini to start ALL apps as a new process

 Note the argc, argv folllows the C main convention that argv[0] is the executable name.
 */
extern void dsn_run(int argc, char **argv, bool is_server = false);

/*!
 exit the process with the given exit code

 \param code exit code for the process

 rDSN runtime does not provide elegant exit routines. Thereafter, developers call dsn_exit
 to exit the current process to avoid exceptions happending during normal exit.
 */
[[noreturn]] extern void dsn_exit(int code);
