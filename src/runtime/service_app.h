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
 *     application model atop zion in c++
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"

#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "utils/rpc_address.h"
#include "common/gpid.h"
#include "utils/factory_store.h"
#include <vector>
#include <string>

namespace dsn {
/*!
@addtogroup app-model
@{
*/

struct service_app_info
{
    int entity_id;
    int index;
    std::string role_name;
    std::string full_name;
    std::string type;
    std::string data_dir;
};

class service_app
{
public:
    template <typename T>
    static service_app *create(const service_app_info *info)
    {
        return new T(info);
    }
    template <typename T>
    static void register_factory(const char *name)
    {
        utils::factory_store<service_app>::register_factory(name, create<T>, PROVIDER_TYPE_MAIN);
    }
    static service_app *new_service_app(const std::string &type, const service_app_info *info);

    static const service_app_info &current_service_app_info();
    static void get_all_service_apps(std::vector<service_app *> *apps);

public:
    service_app(const service_app_info *info);
    virtual ~service_app() {}
    // The back two members of args are version and git commit code, which are passed to
    // version_http_service.
    virtual error_code start(const std::vector<std::string> &args) { return ERR_OK; }
    virtual error_code stop(bool cleanup = false) { return ERR_OK; }
    virtual void on_intercepted_request(gpid pid, bool is_write, dsn::message_ex *msg)
    {
        CHECK(false, "not supported");
    }

    bool is_started() const { return _started; }
    rpc_address primary_address() const { return _address; }
    void set_address(const rpc_address &addr) { _address = addr; }
    void set_started(bool start_flag) { _started = start_flag; }
    const service_app_info &info() const;

protected:
    const service_app_info *const _info;
    rpc_address _address;
    bool _started;
};

/*@}*/
} // namespace dsn
