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

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "failure_detector/failure_detector.h"
#include "failure_detector/failure_detector_multimaster.h"
#include "fd_types.h"
#include "gtest/gtest.h"
#include "meta/meta_options.h"
#include "meta/meta_server_failure_detector.h"
#include "replica/replica_stub.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/group_address.h"
#include "runtime/rpc/network.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_spec.h"
#include "utils/enum_helper.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"
#include "utils/zlocks.h"

DSN_DECLARE_int32(max_succssive_unstable_restart);

using namespace dsn;
using namespace dsn::fd;

DSN_DECLARE_uint64(stable_rs_min_running_seconds);

#define MPORT_START 30001
#define WPORT 40001
#define MCOUNT 3

DEFINE_TASK_CODE_RPC(RPC_MASTER_CONFIG, TASK_PRIORITY_COMMON, THREAD_POOL_FD)

std::atomic_int started_apps(0);
class worker_fd_test : public ::dsn::dist::slave_failure_detector_with_multimaster
{
private:
    volatile bool _send_ping_switch;
    /* this function only triggerd once*/
    std::function<void(rpc_address addr)> _connected_cb;
    std::function<void(const std::vector<rpc_address> &)> _disconnected_cb;

protected:
    virtual void send_beacon(::dsn::rpc_address node, uint64_t time) override
    {
        if (_send_ping_switch)
            failure_detector::send_beacon(node, time);
        else {
            LOG_DEBUG("ignore send beacon, to node[{}], time[{}]", node, time);
        }
    }

    virtual void on_master_disconnected(const std::vector<rpc_address> &nodes) override
    {
        if (_disconnected_cb)
            _disconnected_cb(nodes);
    }

    virtual void on_master_connected(rpc_address node) override
    {
        if (_connected_cb)
            _connected_cb(node);
    }

public:
    worker_fd_test(replication::replica_stub *stub, std::vector<dsn::rpc_address> &meta_servers)
        : slave_failure_detector_with_multimaster(meta_servers,
                                                  [=]() { stub->on_meta_server_disconnected(); },
                                                  [=]() { stub->on_meta_server_connected(); })
    {
        _send_ping_switch = false;
    }
    void toggle_send_ping(bool toggle) { _send_ping_switch = toggle; }
    void when_connected(const std::function<void(rpc_address addr)> &func) { _connected_cb = func; }
    void when_disconnected(const std::function<void(const std::vector<rpc_address> &nodes)> &func)
    {
        _disconnected_cb = func;
    }
    void clear()
    {
        _connected_cb = {};
        _disconnected_cb = {};
    }
};

class master_fd_test : public replication::meta_server_failure_detector
{
private:
    std::function<void(rpc_address addr)> _connected_cb;
    std::function<void(const std::vector<rpc_address> &)> _disconnected_cb;
    volatile bool _response_ping_switch;

public:
    virtual void on_ping(const beacon_msg &beacon, ::dsn::rpc_replier<beacon_ack> &reply) override
    {
        if (_response_ping_switch)
            meta_server_failure_detector::on_ping(beacon, reply);
        else {
            LOG_DEBUG("ignore on ping, beacon msg, time[{}], from[{}], to[{}]",
                      beacon.time,
                      beacon.from_addr,
                      beacon.to_addr);
        }
    }

    virtual void on_worker_disconnected(const std::vector<rpc_address> &worker_list) override
    {
        if (_disconnected_cb)
            _disconnected_cb(worker_list);
    }
    virtual void on_worker_connected(rpc_address node) override
    {
        if (_connected_cb)
            _connected_cb(node);
    }
    master_fd_test() : meta_server_failure_detector(rpc_address(), false)
    {
        _response_ping_switch = true;
    }
    void toggle_response_ping(bool toggle) { _response_ping_switch = toggle; }
    void when_connected(const std::function<void(rpc_address addr)> &func) { _connected_cb = func; }
    void when_disconnected(const std::function<void(const std::vector<rpc_address> &nodes)> &func)
    {
        _disconnected_cb = func;
    }
    void test_register_worker(rpc_address node)
    {
        zauto_lock l(failure_detector::_lock);
        register_worker(node);
    }
    void clear()
    {
        _connected_cb = {};
        _disconnected_cb = {};
    }
};

class test_worker : public service_app, public serverlet<test_worker>
{
public:
    test_worker(const service_app_info *info) : service_app(info), serverlet("test_worker") {}

    error_code start(const std::vector<std::string> &args) override
    {
        std::vector<rpc_address> master_group;
        for (int i = 0; i < 3; ++i)
            master_group.push_back(rpc_address("localhost", MPORT_START + i));
        _worker_fd = new worker_fd_test(nullptr, master_group);
        _worker_fd->start(1, 1, 9, 10);
        ++started_apps;

        register_rpc_handler(
            RPC_MASTER_CONFIG, "RPC_MASTER_CONFIG", &test_worker::on_master_config);
        return ERR_OK;
    }

    error_code stop(bool) override { return ERR_OK; }

    void on_master_config(const config_master_message &request, bool &response)
    {
        LOG_DEBUG("master config, request: {}, type: {}",
                  request.master,
                  request.is_register ? "reg" : "unreg");
        if (request.is_register)
            _worker_fd->register_master(request.master);
        else
            _worker_fd->unregister_master(request.master);
        response = true;
    }

    worker_fd_test *fd() { return _worker_fd; }
private:
    worker_fd_test *_worker_fd;
};

class test_master : public service_app
{
public:
    test_master(const service_app_info *info) : ::dsn::service_app(info) {}

    error_code start(const std::vector<std::string> &args) override
    {
        FLAGS_stable_rs_min_running_seconds = 10;
        FLAGS_max_succssive_unstable_restart = 10;

        _master_fd = new master_fd_test();
        _master_fd->set_options(&_opts);
        bool use_allow_list = false;
        if (args.size() >= 3 && args[1] == "whitelist") {
            std::vector<std::string> ports;
            utils::split_args(args[2].c_str(), ports, ',');
            for (auto &port : ports) {
                rpc_address addr;
                addr.assign_ipv4(network::get_local_ipv4(), std::stoi(port));
                _master_fd->add_allow_list(addr);
            }
            use_allow_list = true;
        }

        _master_fd->start(1, 1, 9, 10, use_allow_list);
        LOG_DEBUG("{}", _master_fd->get_allow_list({}));
        ++started_apps;

        return ERR_OK;
    }

    error_code stop(bool) override { return ERR_OK; }

    master_fd_test *fd() { return _master_fd; }
private:
    master_fd_test *_master_fd;
    replication::fd_suboptions _opts;
};

bool spin_wait_condition(const std::function<bool()> &pred, int seconds)
{
    for (int i = 0; i != seconds; ++i) {
        if (pred())
            return true;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return pred();
}

void fd_test_init()
{
    dsn::service_app::register_factory<test_worker>("worker");
    dsn::service_app::register_factory<test_master>("master");
    srand(time(0));
}

bool get_worker_and_master(test_worker *&worker, std::vector<test_master *> &masters)
{
    bool ans = spin_wait_condition([]() { return started_apps == MCOUNT + 1; }, 30);
    if (!ans)
        return false;

    std::vector<service_app *> apps;
    service_app::get_all_service_apps(&apps);
    masters.resize(MCOUNT, nullptr);
    worker = nullptr;

    for (int i = 0; i != apps.size(); ++i) {
        if (apps[i]->info().type == "worker") {
            if (worker != nullptr)
                return false;
            worker = reinterpret_cast<test_worker *>(apps[i]);
        } else if (apps[i]->info().type == "master") {
            int index = apps[i]->info().index - 1;
            if (index >= masters.size() || masters[index] != nullptr)
                return false;
            masters[index] = reinterpret_cast<test_master *>(apps[i]);
        }
    }

    for (test_master *m : masters)
        if (m == nullptr)
            return false;
    return true;
}

void master_group_set_leader(std::vector<test_master *> &master_group, int leader_index)
{
    rpc_address leader_addr("localhost", MPORT_START + leader_index);
    int i = 0;
    for (test_master *&master : master_group) {
        master->fd()->set_leader_for_test(leader_addr, leader_index == i);
        i++;
    }
}

void worker_set_leader(test_worker *worker, int leader_contact)
{
    worker->fd()->set_leader_for_test(rpc_address("localhost", MPORT_START + leader_contact));

    config_master_message msg;
    msg.master = rpc_address("localhost", MPORT_START + leader_contact);
    msg.is_register = true;
    error_code err;
    bool response;
    std::tie(err, response) = rpc::call_wait<bool>(
        rpc_address("localhost", WPORT), dsn::task_code(RPC_MASTER_CONFIG), msg);
    ASSERT_EQ(err, ERR_OK);
}

void clear(test_worker *worker, std::vector<test_master *> masters)
{
    rpc_address leader = worker->fd()->get_servers().group_address()->leader();

    config_master_message msg;
    msg.master = leader;
    msg.is_register = false;
    error_code err;
    bool response;
    std::tie(err, response) = rpc::call_wait<bool>(
        rpc_address("localhost", WPORT), dsn::task_code(RPC_MASTER_CONFIG), msg);
    ASSERT_EQ(err, ERR_OK);

    worker->fd()->toggle_send_ping(false);

    std::for_each(masters.begin(), masters.end(), [](test_master *mst) {
        mst->fd()->clear_workers();
        mst->fd()->toggle_response_ping(true);
    });
}

void finish(test_worker *worker, test_master *master, int master_index)
{
    LOG_WARNING("start to finish");
    std::atomic_int wait_count;
    wait_count.store(2);
    worker->fd()->when_disconnected(
        [&wait_count, master_index](const std::vector<rpc_address> &addr_list) mutable {
            ASSERT_EQ(addr_list.size(), 1);
            ASSERT_EQ(addr_list[0].port(), MPORT_START + master_index);
            --wait_count;
        });

    master->fd()->when_disconnected(
        [&wait_count](const std::vector<rpc_address> &addr_list) mutable {
            ASSERT_EQ(addr_list.size(), 1);
            ASSERT_EQ(addr_list[0].port(), WPORT);
            --wait_count;
        });

    // we don't send any ping message now
    worker->fd()->toggle_send_ping(false);
    ASSERT_TRUE(spin_wait_condition([&wait_count] { return wait_count == 0; }, 20));
    worker->fd()->clear();
    master->fd()->clear();
}

TEST(fd, dummy_connect_disconnect)
{
    test_worker *worker;
    std::vector<test_master *> masters;
    ASSERT_TRUE(get_worker_and_master(worker, masters));

    clear(worker, masters);
    // set master with smallest index as the leader
    master_group_set_leader(masters, 0);
    // set the worker contact leader
    worker_set_leader(worker, 0);

    test_master *leader = masters[0];
    // simply wait for two connected
    std::atomic_int wait_count;
    wait_count.store(2);
    worker->fd()->when_connected([&wait_count](rpc_address leader) mutable {
        ASSERT_EQ(leader.port(), MPORT_START);
        --wait_count;
    });
    leader->fd()->when_connected([&wait_count](rpc_address worker_addr) mutable {
        ASSERT_EQ(worker_addr.port(), WPORT);
        --wait_count;
    });

    worker->fd()->toggle_send_ping(true);
    ASSERT_TRUE(spin_wait_condition([&wait_count] { return wait_count == 0; }, 20));

    finish(worker, leader, 0);
}

TEST(fd, master_redirect)
{
    test_worker *worker;
    std::vector<test_master *> masters;
    ASSERT_TRUE(get_worker_and_master(worker, masters));

    int index = masters.size() - 1;

    clear(worker, masters);
    /* leader is the last master*/
    master_group_set_leader(masters, index);
    // we contact to 0
    worker_set_leader(worker, 0);

    test_master *leader = masters[index];
    std::atomic_int wait_count;
    wait_count.store(2);
    /* although we contact to the first master, but in the end we must connect to the right leader
     */
    worker->fd()->when_connected([&wait_count](rpc_address leader) mutable { --wait_count; });
    leader->fd()->when_connected([&wait_count](rpc_address worker_addr) mutable {
        ASSERT_EQ(worker_addr.port(), WPORT);
        --wait_count;
    });

    worker->fd()->toggle_send_ping(true);
    ASSERT_TRUE(spin_wait_condition([&wait_count] { return wait_count == 0; }, 20));
    // in the end, the worker will connect to the right master
    ASSERT_TRUE(spin_wait_condition(
        [worker, index] {
            return worker->fd()->current_server_contact().port() == MPORT_START + index;
        },
        20));

    finish(worker, leader, index);
}

TEST(fd, switch_new_master_suddenly)
{
    test_worker *worker;
    std::vector<test_master *> masters;
    ASSERT_TRUE(get_worker_and_master(worker, masters));

    clear(worker, masters);

    test_master *tst_master;
    int index = 0;

    master_group_set_leader(masters, index);
    // and now we contact to 1
    worker_set_leader(worker, 1);

    tst_master = masters[index];
    std::atomic_int wait_count;
    wait_count.store(2);

    auto cb = [&wait_count](rpc_address) mutable { --wait_count; };
    worker->fd()->when_connected(cb);
    tst_master->fd()->when_connected(cb);

    worker->fd()->toggle_send_ping(true);
    ASSERT_TRUE(spin_wait_condition([&wait_count]() { return wait_count == 0; }, 20));
    ASSERT_EQ(worker->fd()->current_server_contact().port(), MPORT_START + index);

    worker->fd()->when_connected(nullptr);
    /* we select a new leader */
    index = masters.size() - 1;
    tst_master = masters[index];
    /*
     * for perfect FD, the new master should assume the worker connected.
     * But first we test if the worker can connect to the new master.
     * So clear all the workers
     */
    tst_master->fd()->clear_workers();
    wait_count.store(1);
    tst_master->fd()->when_connected([&wait_count](rpc_address addr) mutable {
        ASSERT_EQ(addr.port(), WPORT);
        --wait_count;
    });
    master_group_set_leader(masters, index);

    /* now we can worker the worker to connect to the new master */
    ASSERT_TRUE(spin_wait_condition([&wait_count]() { return wait_count == 0; }, 20));
    /* it may takes time for worker to switch to new master, but 20 seconds
     * is enough as in our setting, lease_period is 9 seconds. */
    ASSERT_TRUE(spin_wait_condition(
        [worker, index]() {
            return worker->fd()->current_server_contact().port() == MPORT_START + index;
        },
        20));

    finish(worker, tst_master, index);
}

TEST(fd, old_master_died)
{
    test_worker *worker;
    std::vector<test_master *> masters;
    ASSERT_TRUE(get_worker_and_master(worker, masters));
    clear(worker, masters);

    test_master *tst_master;
    int index = 0;
    master_group_set_leader(masters, index);
    // and now we contact to 0
    worker_set_leader(worker, 0);

    tst_master = masters[index];
    std::atomic_int wait_count;
    wait_count.store(2);

    auto cb = [&wait_count](rpc_address) mutable { --wait_count; };
    worker->fd()->when_connected(cb);
    tst_master->fd()->when_connected(cb);

    worker->fd()->toggle_send_ping(true);
    ASSERT_TRUE(spin_wait_condition([&wait_count]() -> bool { return wait_count == 0; }, 20));
    ASSERT_EQ(worker->fd()->current_server_contact().port(), MPORT_START + index);

    worker->fd()->when_connected(nullptr);
    tst_master->fd()->when_connected(nullptr);

    worker->fd()->when_disconnected([](const std::vector<rpc_address> &masters_list) {
        ASSERT_EQ(masters_list.size(), 1);
        LOG_DEBUG("disconnect from master: {}", masters_list[0]);
    });

    /*first let's stop the old master*/
    tst_master->fd()->toggle_response_ping(false);
    /* then select a new one */
    index = masters.size() - 1;
    tst_master = masters[index];

    /* only for test */
    tst_master->fd()->clear_workers();
    wait_count.store(1);

    tst_master->fd()->when_connected([&wait_count](rpc_address addr) mutable {
        EXPECT_EQ(addr.port(), WPORT);
        --wait_count;
    });
    master_group_set_leader(masters, index);

    /* now we can wait the worker to connect to the new master */
    ASSERT_TRUE(spin_wait_condition([&wait_count]() { return wait_count == 0; }, 20));
    /* it may takes time for worker to switch to new master, but 20 seconds
     * is enough as in our setting, lease_period is 9 seconds. */
    ASSERT_TRUE(spin_wait_condition(
        [worker, index]() {
            return worker->fd()->current_server_contact().port() == MPORT_START + index;
        },
        20));

    finish(worker, tst_master, index);
}

TEST(fd, worker_died_when_switch_master)
{
    test_worker *worker;
    std::vector<test_master *> masters;
    ASSERT_TRUE(get_worker_and_master(worker, masters));
    clear(worker, masters);

    test_master *tst_master;
    int index = 0;
    master_group_set_leader(masters, index);
    // and now we contact to 0
    worker_set_leader(worker, 0);

    tst_master = masters[index];
    std::atomic_int wait_count;
    wait_count.store(2);

    auto cb = [&wait_count](rpc_address) mutable { --wait_count; };
    worker->fd()->when_connected(cb);
    tst_master->fd()->when_connected(cb);

    worker->fd()->toggle_send_ping(true);
    ASSERT_TRUE(spin_wait_condition([&wait_count]() { return wait_count == 0; }, 20));
    ASSERT_EQ(worker->fd()->current_server_contact().port(), MPORT_START + index);

    worker->fd()->when_connected(nullptr);
    tst_master->fd()->when_connected(nullptr);

    /*first stop the old leader*/
    tst_master->fd()->toggle_response_ping(false);

    /*then select another leader*/
    index = masters.size() - 1;
    tst_master = masters[index];

    wait_count.store(2);
    tst_master->fd()->when_disconnected(
        [&wait_count](const std::vector<rpc_address> &worker_list) mutable {
            ASSERT_EQ(worker_list.size(), 1);
            ASSERT_EQ(worker_list[0].port(), WPORT);
            wait_count--;
        });
    worker->fd()->when_disconnected(
        [&wait_count](const std::vector<rpc_address> &master_list) mutable {
            ASSERT_EQ(master_list.size(), 1);
            wait_count--;
        });

    /* we assume the worker is alive */
    tst_master->fd()->test_register_worker(rpc_address("localhost", WPORT));
    master_group_set_leader(masters, index);

    /* then stop the worker*/
    worker->fd()->toggle_send_ping(false);
    ASSERT_TRUE(spin_wait_condition([&wait_count] { return wait_count == 0; }, 20));
}

dsn::message_ex *create_fake_rpc_response()
{
    dsn::message_ex *req =
        dsn::message_ex::create_received_request(RPC_MASTER_CONFIG, DSF_THRIFT_BINARY, nullptr, 0);
    dsn::message_ex *response = req->create_response();
    req->add_ref();
    req->release_ref();
    return response;
}

TEST(fd, update_stability)
{
    test_worker *worker;
    std::vector<test_master *> masters;
    ASSERT_TRUE(get_worker_and_master(worker, masters));
    clear(worker, masters);

    master_group_set_leader(masters, 0);
    master_fd_test *fd = masters[0]->fd();
    fd->toggle_response_ping(true);

    replication::fd_suboptions opts;
    FLAGS_stable_rs_min_running_seconds = 5;
    FLAGS_max_succssive_unstable_restart = 2;
    fd->set_options(&opts);

    replication::meta_server_failure_detector::stability_map *smap =
        fd->get_stability_map_for_test();
    smap->clear();

    dsn::rpc_replier<beacon_ack> r(create_fake_rpc_response());
    beacon_msg msg;
    msg.from_addr = rpc_address("localhost", 123);
    msg.to_addr = rpc_address("localhost", MPORT_START);
    msg.time = dsn_now_ms();
    msg.__isset.start_time = true;
    msg.start_time = 1000;

    // first on ping
    fd->on_ping(msg, r);
    ASSERT_EQ(1, smap->size());
    ASSERT_NE(smap->end(), smap->find(msg.from_addr));

    replication::meta_server_failure_detector::worker_stability &ws =
        smap->find(msg.from_addr)->second;
    ASSERT_EQ(0, ws.unstable_restart_count);
    ASSERT_EQ(msg.start_time, ws.last_start_time_ms);
    ASSERT_TRUE(r.is_empty());
    r = dsn::rpc_replier<beacon_ack>(create_fake_rpc_response());

    // unstably restart and resend ping
    msg.start_time += 4000;
    fd->on_ping(msg, r);
    ASSERT_EQ(1, ws.unstable_restart_count);
    ASSERT_EQ(msg.start_time, ws.last_start_time_ms);
    ASSERT_TRUE(r.is_empty());
    r = dsn::rpc_replier<beacon_ack>(create_fake_rpc_response());

    // upstably restart and resend ping again, the node's ping will be ignored
    msg.start_time += 4000;
    fd->on_ping(msg, r);
    ASSERT_EQ(msg.start_time, ws.last_start_time_ms);
    ASSERT_EQ(2, ws.unstable_restart_count);
    ASSERT_FALSE(r.is_empty());

    // stably restart, the meta stability & unstable_count will be reset
    msg.start_time += 10000;
    fd->on_ping(msg, r);
    ASSERT_EQ(msg.start_time, ws.last_start_time_ms);
    ASSERT_EQ(0, ws.unstable_restart_count);
    ASSERT_TRUE(r.is_empty());
    r = dsn::rpc_replier<beacon_ack>(create_fake_rpc_response());

    // unstably restart, unstable-count++
    msg.start_time += 4000;
    fd->on_ping(msg, r);
    ASSERT_EQ(msg.start_time, ws.last_start_time_ms);
    ASSERT_EQ(1, ws.unstable_restart_count);
    ASSERT_TRUE(r.is_empty());
    r = dsn::rpc_replier<beacon_ack>(create_fake_rpc_response());

    // not restart, unstable-count will be reset
    fd->on_ping(msg, r);
    ASSERT_EQ(msg.start_time, ws.last_start_time_ms);
    ASSERT_EQ(0, ws.unstable_restart_count);
    ASSERT_TRUE(r.is_empty());
    r = dsn::rpc_replier<beacon_ack>(create_fake_rpc_response());

    // old message, will be ignored
    msg.start_time -= 4000;
    fd->on_ping(msg, r);
    ASSERT_EQ(msg.start_time + 4000, ws.last_start_time_ms);
    ASSERT_EQ(0, ws.unstable_restart_count);
    ASSERT_TRUE(r.is_empty());
    r = dsn::rpc_replier<beacon_ack>(create_fake_rpc_response());

    // unstable restart, unstable-count++
    msg.start_time += 8000;
    fd->on_ping(msg, r);
    ASSERT_EQ(msg.start_time, ws.last_start_time_ms);
    ASSERT_EQ(1, ws.unstable_restart_count);
    ASSERT_TRUE(r.is_empty());
    r = dsn::rpc_replier<beacon_ack>(create_fake_rpc_response());

    // unstable restart, unstable-count++, node's ping will be ignored
    msg.start_time += 4000;
    fd->on_ping(msg, r);
    ASSERT_EQ(msg.start_time, ws.last_start_time_ms);
    ASSERT_EQ(2, ws.unstable_restart_count);
    ASSERT_FALSE(r.is_empty());

    // reset stat
    fd->reset_stability_stat(msg.from_addr);
    ASSERT_EQ(msg.start_time, ws.last_start_time_ms);
    ASSERT_EQ(0, ws.unstable_restart_count);
}

TEST(fd, not_in_whitelist)
{
    test_worker *worker;
    std::vector<test_master *> masters;
    ASSERT_TRUE(get_worker_and_master(worker, masters));

    clear(worker, masters);
    // set master with smallest index as the leader
    master_group_set_leader(masters, 0);
    // set the worker contact leader
    worker_set_leader(worker, 0);

    std::atomic_int wait_count;
    wait_count.store(1);
    auto cb = [&wait_count](rpc_address) mutable { --wait_count; };
    worker->fd()->when_connected(cb);
    worker->fd()->toggle_send_ping(true);

    ASSERT_TRUE(spin_wait_condition([&wait_count] { return wait_count == 1; }, 20));
}
