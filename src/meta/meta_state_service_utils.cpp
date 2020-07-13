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

#include <dsn/cpp/pipeline.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication.h>

#include "meta_state_service_utils.h"
#include "meta_state_service_utils_impl.h"

namespace dsn {
namespace replication {
namespace mss {

meta_storage::meta_storage(dist::meta_state_service *remote_storage, task_tracker *tracker)
    : _remote(remote_storage), _tracker(tracker)
{
    dassert(tracker != nullptr, "must set task tracker");
}

meta_storage::~meta_storage() = default;

void meta_storage::create_node_recursively(std::queue<std::string> &&nodes,
                                           blob &&value,
                                           std::function<void()> &&cb)
{
    dassert(!nodes.empty(), "");

    on_create_recursively op;
    op.initialize(this);
    op.args.reset(
        new on_create_recursively::arguments{std::move(cb), std::move(value), std::move(nodes)});
    op.run();
}

void meta_storage::create_node(std::string &&node, blob &&value, std::function<void()> &&cb)
{
    on_create op;
    op.initialize(this);
    op.args.reset(new on_create::arguments{std::move(cb), std::move(value), std::move(node)});
    op.run();
}

void meta_storage::delete_node_recursively(std::string &&node, std::function<void()> &&cb)
{
    delete_node_impl(std::move(node), std::move(cb), true);
}

void meta_storage::delete_node(std::string &&node, std::function<void()> &&cb)
{
    delete_node_impl(std::move(node), std::move(cb), false);
}

void meta_storage::delete_node_impl(std::string &&node,
                                    std::function<void()> &&cb,
                                    bool is_recursive)
{
    on_delete op;
    op.initialize(this);
    op.args.reset(new on_delete::arguments);
    op.args->cb = std::move(cb);
    op.args->node = std::move(node);
    op.args->is_recursively_delete = is_recursive;
    op.run();
}

void meta_storage::set_data(std::string &&node, blob &&value, std::function<void()> &&cb)
{
    on_set_data op;
    op.initialize(this);
    op.args.reset(new on_set_data::arguments{std::move(cb), std::move(node), std::move(value)});
    op.run();
}

void meta_storage::get_data(std::string &&node, std::function<void(const blob &)> &&cb)
{
    on_get_data op;
    op.initialize(this);
    op.args.reset(new on_get_data::arguments{std::move(cb), std::move(node)});
    op.run();
}

void meta_storage::get_children(std::string &&node,
                                std::function<void(bool, const std::vector<std::string> &)> &&cb)
{
    on_get_children op;
    op.initialize(this);
    op.args.reset(new on_get_children::arguments{std::move(cb), std::move(node)});
    op.run();
}

} // namespace mss
} // namespace replication
} // namespace dsn
