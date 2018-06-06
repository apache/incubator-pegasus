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

#include <dsn/cpp/pipeline.h>
#include <dsn/dist/fmt_logging.h>

#include "meta_state_service_utils.h"

namespace dsn {
namespace replication {
namespace mss {

struct op_type
{
    enum type
    {
        OP_NONE,

        OP_CREATE_RECURSIVELY,
        OP_CREATE,
        OP_DELETE_RECURSIVELY,
        OP_DELETE,
        OP_SET_DATA,
        OP_GET_DATA,
        OP_GET_CHILDREN,
    };

    static const char *to_string(type v)
    {
        static const char *op_type_to_string_map[] = {
            "OP_CREATE_RECURSIVELY",
            "OP_CREATE",
            "OP_DELETE_RECURSIVELY",
            "OP_DELETE",
            "OP_SET_DATA",
            "OP_GET_DATA",
            "OP_GET_CHILDREN",
        };

        dassert_f(v != OP_NONE && v <= (sizeof(op_type_to_string_map) / sizeof(char *)),
                  "invalid type: {}",
                  v);
        return op_type_to_string_map[v - 1];
    }
};

/// Base class for all operations.
struct operation : pipeline::environment
{
    void initialize(meta_storage *storage)
    {
        _ms = storage;
        task_tracker(storage->_tracker).thread_pool(LPC_META_STATE_HIGH);
    }

    // The common strategy for error handling:
    // retry after 1 sec if timeout, or terminate.
    template <typename T>
    void on_error(T *this_instance, op_type::type type, error_code ec, const std::string &path)
    {
        if (ec == ERR_TIMEOUT) {
            dwarn_f("request({}) on path({}) was timeout, retry after 1 second",
                    op_type::to_string(type),
                    path);
            pipeline::repeat(std::move(*this_instance), 1_s);
            return;
        }
        dfatal_f("request({}) on path({}) encountered an unexpected error({})",
                 op_type::to_string(type),
                 path,
                 ec.to_string());
    }

    dist::meta_state_service *remote_storage() const { return _ms->_remote; }

    dsn::task_tracker *tracker() const { return _ms->_tracker; }

private:
    meta_storage *_ms{nullptr};
};

// Developer Notes:
//
// As a concern of performance, arguments are wrapped into a shared_ptr to be used
// in callback of meta_state_service without copying.
//
// To be able to repeat the internal task using pipeline::repeat, the operations must
// implement `void run()` method.
//

struct on_create_recursively : operation
{
    struct arguments
    {
        std::function<void()> cb;
        dsn::blob val;
        std::queue<std::string> nodes;
    };
    std::shared_ptr<arguments> args;

    // ASSERTED: !args->nodes.empty
    void run()
    {
        // first node
        if (_cur_path.empty()) { // first node requires leading '/'
            _cur_path += args->nodes.front();
            args->nodes.pop();
        }

        remote_storage()->create_node(_cur_path,
                                      LPC_META_STATE_HIGH,
                                      [op = *this](error_code ec) mutable { op.on_error(ec); },
                                      args->nodes.empty() ? args->val : blob(),
                                      tracker());
    }

    void on_error(error_code ec)
    {
        if (ec == ERR_OK || ec == ERR_NODE_ALREADY_EXIST) {
            // create next node
            if (!args->nodes.empty()) {
                _cur_path += "/" + args->nodes.front();
                args->nodes.pop();
                pipeline::repeat(std::move(*this));
            } else {
                args->cb();
                _cur_path.clear();
            }
            return;
        }
        operation::on_error(this, op_type::OP_CREATE_RECURSIVELY, ec, _cur_path);
    }

private:
    std::string _cur_path;
};

struct on_create : operation
{
    struct arguments
    {
        std::function<void()> cb;
        dsn::blob val;
        std::string node;
    };
    std::shared_ptr<arguments> args;

    void run()
    {
        remote_storage()->create_node(args->node,
                                      LPC_META_STATE_HIGH,
                                      [op = *this](error_code ec) mutable { op.on_error(ec); },
                                      args->val,
                                      tracker());
    }

    void on_error(error_code ec)
    {
        if (ec == ERR_OK || ec == ERR_NODE_ALREADY_EXIST) {
            args->cb();
            return;
        }

        operation::on_error(this, op_type::OP_CREATE, ec, args->node);
    }
};

struct on_delete : operation
{
    struct arguments
    {
        std::function<void()> cb;
        std::string node;
        bool is_recursively_delete{false};
    };
    std::shared_ptr<arguments> args;

    void run()
    {
        remote_storage()->delete_node(args->node,
                                      args->is_recursively_delete,
                                      LPC_META_STATE_HIGH,
                                      [op = *this](error_code ec) mutable { op.on_error(ec); },
                                      tracker());
    }

    void on_error(error_code ec)
    {
        if (ec == ERR_OK || ec == ERR_OBJECT_NOT_FOUND) {
            args->cb();
            return;
        }

        auto type =
            args->is_recursively_delete ? op_type::OP_DELETE_RECURSIVELY : op_type::OP_DELETE;
        operation::on_error(this, type, ec, args->node);
    }
};

struct on_get_data : operation
{
    struct arguments
    {
        std::function<void(const blob &)> cb;
        std::string node;
    };
    std::shared_ptr<arguments> args;

    void run()
    {
        remote_storage()->get_data(
            args->node,
            LPC_META_STATE_HIGH,
            [op = *this](error_code ec, const blob &val) mutable { op.on_error(ec, val); },
            tracker());
    }

    void on_error(error_code ec, const blob &val)
    {
        if (ec == ERR_OK || ec == ERR_OBJECT_NOT_FOUND) {
            args->cb(val);
            return;
        }
        operation::on_error(this, op_type::OP_GET_DATA, ec, args->node);
    }
};

struct on_set_data : operation
{
    struct arguments
    {
        std::function<void()> cb;
        std::string node;
        dsn::blob val;
    };
    std::shared_ptr<arguments> args;

    void run()
    {
        remote_storage()->set_data(args->node,
                                   args->val,
                                   LPC_META_STATE_HIGH,
                                   [op = *this](error_code ec) mutable { op.on_error(ec); },
                                   tracker());
    }

    void on_error(error_code ec)
    {
        if (ec == ERR_OK) {
            args->cb();
            return;
        }

        operation::on_error(this, op_type::OP_SET_DATA, ec, args->node);
    }
};

struct on_get_children : operation
{
    struct arguments
    {
        std::function<void(bool, const std::vector<std::string> &)> cb;
        std::string node;
    };
    std::shared_ptr<arguments> args;

    void run()
    {
        remote_storage()->get_children(
            args->node,
            LPC_META_STATE_HIGH,
            [op = *this](error_code ec, const std::vector<std::string> &children) mutable {
                op.on_error(ec, children);
            },
            tracker());
    }

    void on_error(error_code ec, const std::vector<std::string> &children)
    {
        if (ec == ERR_OK) {
            args->cb(true, children);
            return;
        }
        if (ec == ERR_OBJECT_NOT_FOUND) {
            args->cb(false, children);
            return;
        }
        operation::on_error(this, op_type::OP_GET_CHILDREN, ec, args->node);
    }
};

} // namespace mss
} // namespace replication
} // namespace dsn
