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

#pragma once

#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "meta/meta_state_service.h"
#include "task/future_types.h"
#include "task/task.h"
#include "task/task_code.h"
#include "task/task_tracker.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/synchronize.h"

namespace dsn {
namespace dist {

class zookeeper_session;

class meta_state_service_zookeeper : public meta_state_service, public ref_counter
{
public:
    explicit meta_state_service_zookeeper();
    virtual ~meta_state_service_zookeeper() override;

    // no parameter need
    virtual error_code initialize(const std::vector<std::string> &args) override;
    virtual error_code finalize() override;

    virtual std::shared_ptr<meta_state_service::transaction_entries>
    new_transaction_entries(unsigned int capacity) override;

    virtual task_ptr
    submit_transaction(const std::shared_ptr<meta_state_service::transaction_entries> &entries,
                       task_code cb_code,
                       const err_callback &cb_transaction,
                       task_tracker *tracker = nullptr) override;

    virtual task_ptr create_node(const std::string &node,
                                 task_code cb_code,
                                 const err_callback &cb_create,
                                 const blob &value = blob(),
                                 dsn::task_tracker *tracker = nullptr) override;

    virtual task_ptr delete_node(const std::string &node,
                                 bool recursively_delete,
                                 task_code cb_code,
                                 const err_callback &cb_delete,
                                 dsn::task_tracker *tracker = nullptr) override;

    virtual task_ptr node_exist(const std::string &node,
                                task_code cb_code,
                                const err_callback &cb_exist,
                                dsn::task_tracker *tracker = nullptr) override;

    virtual task_ptr get_data(const std::string &node,
                              task_code cb_code,
                              const err_value_callback &cb_get_data,
                              dsn::task_tracker *tracker = nullptr) override;

    virtual task_ptr set_data(const std::string &node,
                              const blob &value,
                              task_code cb_code,
                              const err_callback &cb_set_data,
                              dsn::task_tracker *tracker = nullptr) override;

    virtual task_ptr get_children(const std::string &node,
                                  task_code cb_code,
                                  const err_stringv_callback &cb_get_children,
                                  dsn::task_tracker *tracker = nullptr) override;

    task_ptr delete_empty_node(const std::string &node,
                               task_code cb_code,
                               const err_callback &cb_delete,
                               dsn::task_tracker *tracker);
    int hash() const { return (int)(((uint64_t)this) & 0xffffffff); }

private:
    typedef ref_ptr<meta_state_service_zookeeper> ref_this;

    bool _first_call;
    int _zoo_state;
    zookeeper_session *_session;
    utils::notify_event _notifier;

    dsn::task_tracker _tracker;

    static void on_zoo_session_evt(ref_this ptr, int zoo_state);
    static void visit_zookeeper_internal(ref_this ptr,
                                         task_ptr callback,
                                         void *result /*zookeeper_session::zoo_opcontext**/);
};
} // namespace dist
} // namespace dsn
