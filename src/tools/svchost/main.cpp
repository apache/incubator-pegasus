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

# include <dsn/service_api_c.h>

//
// this svchost is a loader than can run all
// rDSN-based service modules with configuration as below.
//
// [apps.xxx]
// dmodule = echo //.so, .dll will be automatically appended
// count = 1
// arguments = xxx
// 
// dsn_run will load all these *dmodule*s which automatically
// register their correspondent app roles into the system
// using dsn_register_app_role, and run them as specified.
//

int main(int argc, char** argv)
{
    //
    // run the system with arguments
    //   config [-cargs k1=v1;k2=v2] [-app app_name] [-app_index index]
    // e.g., config.ini -app replica -app_index 1 to start the first replica as a new process
    //       config.ini -app replica to start ALL replicas (count specified in config) as a new process
    //       config.ini -app replica -cargs replica-port=34556 to start ALL replicas with given port variable specified in config.ini
    //       config.ini to start ALL apps as a new process
    //
    dsn_run(argc, argv, true);
    return 0;
}
