// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <sstream>

class global_env
{
public:
    std::string _pegasus_root;
    std::string _working_dir;
    std::string _host_ip;

    static global_env &instance() { return inst; }

private:
    global_env();
    global_env(const global_env &other) = delete;
    global_env(global_env &&other) = delete;

    void get_hostip();
    void get_dirs();
    static global_env inst;
};
