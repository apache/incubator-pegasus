#! /usr/bin/env python
# coding=utf-8

import time
from subprocess import check_output, CalledProcessError, STDOUT

from twisted.trial import unittest

from pypegasus.pgclient import *


def getstatusoutput(cmd):
    try:
        data = check_output(cmd, shell=True, universal_newlines=True, stderr=STDOUT)
        status = 0
    except CalledProcessError as ex:
        data = ex.output
        status = ex.returncode
    if data[-1:] == '\n':
        data = data[:-1]
    return status, data


class ServerOperator(object):
    shell_path = '/your/pegasus-shell/dir'

    @classmethod
    def modify_conf(cls, old_conf, new_conf):
        origin_conf_file = cls.shell_path + '/src/server/config-server.ini'
        status, output = getstatusoutput('sed -i "s/%s/%s/" %s'
                                         % (old_conf, new_conf, origin_conf_file))
        # print(status, output)

    @classmethod
    def start_cluster(cls, meta_count, replica_count, check_health):
        status, output = getstatusoutput('cd %s && ./run.sh start_onebox -m %s -r %s'
                                         % (cls.shell_path, meta_count, replica_count))
        if check_health:
            cls.wait_until_cluster_health()
        else:
            time.sleep(1)  # wait a while for meta ready

    @classmethod
    def stop_and_clear_cluster(cls):
        status, output = getstatusoutput('cd %s && ./run.sh stop_onebox'
                                         % cls.shell_path)

        status, output = getstatusoutput('cd %s && ./run.sh clear_onebox'
                                         % cls.shell_path)

    @classmethod
    def operate_1_server(cls, op_type, server_type, index):
        status, output = getstatusoutput('cd %s && ./run.sh %s_onebox_instance -%s %s'
                                         % (cls.shell_path, op_type, server_type, index))
        # print(status, output)

    @classmethod
    def stop_1_replica(cls, index):
        cls.operate_1_server('stop', 'r', index)

    @classmethod
    def start_1_replica(cls, index):
        cls.operate_1_server('start', 'r', index)

    @classmethod
    def restart_1_replica(cls, index):
        cls.operate_1_server('restart', 'r', index)

    @classmethod
    def stop_1_meta(cls, index):
        cls.operate_1_server('stop', 'm', index)

    @classmethod
    def start_1_meta(cls, index):
        cls.operate_1_server('start', 'm', index)

    @classmethod
    def wait_until_cluster_health(cls):
        cmd = ('cd %s && echo "app temp -d" | ./run.sh shell |'
               ' grep fully_healthy_partition_count | awk \'{print $NF}\''
               % cls.shell_path)
        while True:
            status, output = getstatusoutput(cmd)
            # 0 means return value, 8 means fully_healthy_partition_count = 8
            if status == 0 and output == '8':
                break


class TestIntegration(unittest.TestCase):
    TEST_HKEY = 'test_hkey_1'
    TEST_SKEY = 'test_skey_1'
    TEST_VALUE = 'test_value_1'
    DATA_COUNT = 500
    MAX_RETRY_COUNT = 30
    check_health = True

    def setUp(self):
        ServerOperator.stop_and_clear_cluster()

    def tearDown(self):
        self.c.close()
        ServerOperator.stop_and_clear_cluster()

    @inlineCallbacks
    def init(self, meta_count, replica_count, confs=None):
        if isinstance(confs, dict):
            for old_conf, new_conf in confs.items():
                ServerOperator.modify_conf(old_conf, new_conf)
        ServerOperator.start_cluster(meta_count, replica_count, self.check_health)
        self.c = Pegasus(['127.0.0.1:34601', '127.0.0.1:34602', '127.0.0.1:34603'], 'temp')
        ret = yield self.c.init()
        self.assertTrue(ret)

    @inlineCallbacks
    def loop_op(self):
        for i in range(self.DATA_COUNT):
            ret = yield self.c.get(self.TEST_HKEY + str(i), self.TEST_SKEY, 1000)
            if not isinstance(ret, tuple) or ret[0] != error_types.ERR_OK.value or bytes.decode(ret[1]) != self.TEST_VALUE:
                defer.returnValue(False)
        defer.returnValue(True)

    @inlineCallbacks
    def check_data(self):
        wait_times = 0
        while True:
            ret = yield self.loop_op()
            if not ret:
                wait_times += 1
                time.sleep(1)
                if wait_times >= self.MAX_RETRY_COUNT:
                    self.assertTrue(False)
            else:
                break

    @inlineCallbacks
    def test_replica_start(self):
        yield self.init(3, 3)
        (ret, ign) = yield self.c.set(self.TEST_HKEY, self.TEST_SKEY, self.TEST_VALUE)
        self.assertEqual(ret, error_types.ERR_OK.value)

    @inlineCallbacks
    def test_can_not_connect(self):
        self.c = Pegasus(['127.0.1.1:34601', '127.0.0.1:34602', '127.0.0.1:34603'], 'temp')
        ret = yield self.c.init()
        self.assertEqual(ret, None)

    @inlineCallbacks
    def test_1of3_replica_restart(self):
        yield self.init(3, 3)
        for i in range(self.DATA_COUNT):
            (ret, ign) = yield self.c.set(self.TEST_HKEY + str(i), self.TEST_SKEY, self.TEST_VALUE)
            self.assertEqual(ret, error_types.ERR_OK.value)

        ServerOperator.restart_1_replica(1)

        yield self.check_data()

    @inlineCallbacks
    def test_3of3_replica_restart(self):
        yield self.init(3, 3)
        ServerOperator.wait_until_cluster_health()

        for i in range(self.DATA_COUNT):
            (ret, ign) = yield self.c.set(self.TEST_HKEY + str(i), self.TEST_SKEY, self.TEST_VALUE)
            self.assertEqual(ret, error_types.ERR_OK.value)

        for i in range(1, 4):
            ServerOperator.restart_1_replica(i)

            yield self.check_data()

    @inlineCallbacks
    def test_1of5_replica_restart(self):
        yield self.init(3, 5)
        for i in range(self.DATA_COUNT):
            (ret, ign) = yield self.c.set(self.TEST_HKEY + str(i), self.TEST_SKEY, self.TEST_VALUE)
            self.assertEqual(ret, error_types.ERR_OK.value)

        ServerOperator.restart_1_replica(1)

        yield self.check_data()

    @inlineCallbacks
    def test_1of5_replica_stop_and_start(self):
        yield self.init(3, 5)
        for i in range(self.DATA_COUNT):
            (ret, ign) = yield self.c.set(self.TEST_HKEY + str(i), self.TEST_SKEY, self.TEST_VALUE)
            self.assertEqual(ret, error_types.ERR_OK.value)

        ServerOperator.stop_1_replica(1)

        yield self.check_data()

        ServerOperator.start_1_replica(1)

        yield self.check_data()

    @inlineCallbacks
    def test_5of5_replica_restart(self):
        yield self.init(3, 5)
        ServerOperator.wait_until_cluster_health()

        for i in range(self.DATA_COUNT):
            (ret, ign) = yield self.c.set(self.TEST_HKEY + str(i), self.TEST_SKEY, self.TEST_VALUE)
            self.assertEqual(ret, error_types.ERR_OK.value)

        for i in range(1, 6):
            ServerOperator.restart_1_replica(i)

            yield self.check_data()

    @inlineCallbacks
    def test_1of5_replica_stop(self):
        yield self.init(3, 5)
        for i in range(self.DATA_COUNT):
            (ret, ign) = yield self.c.set(self.TEST_HKEY + str(i), self.TEST_SKEY, self.TEST_VALUE)
            self.assertEqual(ret, error_types.ERR_OK.value)

        ServerOperator.stop_1_replica(1)

        wait_times = 0
        while True:
            ret = yield self.loop_op()
            if not ret:
                wait_times += 1
                time.sleep(wait_times)
                if wait_times >= 20:
                    self.assertTrue(False)
            else:
                break

    @inlineCallbacks
    def test_2of5_replica_stop(self):
        yield self.init(3, 5)
        for i in range(self.DATA_COUNT):
            (ret, ign) = yield self.c.set(self.TEST_HKEY + str(i), self.TEST_SKEY, self.TEST_VALUE)
            self.assertEqual(ret, error_types.ERR_OK.value)

        for i in range(1, 3):
            ServerOperator.stop_1_replica(i)

            yield self.check_data()

    @inlineCallbacks
    def test_1of5_replica_stop_and_start_with_meta_stop_and_start(self):
        confs = {'timeout_ms = 60000': 'timeout_ms = 2000'}
        yield self.init(2, 5, confs)
        for i in range(self.DATA_COUNT):
            (ret, ign) = yield self.c.set(self.TEST_HKEY + str(i), self.TEST_SKEY, self.TEST_VALUE)
            self.assertEqual(ret, error_types.ERR_OK.value)

        for i in range(2):
            ServerOperator.stop_1_replica(1)
            ServerOperator.stop_1_meta(i + 1)
            time.sleep(3)

            yield self.check_data()

            ServerOperator.start_1_meta(i + 1)
            ServerOperator.start_1_replica(1)
            time.sleep(3)

            yield self.check_data()

    @inlineCallbacks
    def test_0_replica_scan_exception(self):
        self.check_health = False
        yield self.init(3, 0)
        o = ScanOptions()
        s = self.c.get_scanner(self.TEST_HKEY, b'\x00\x00', b'\xFF\xFF', o)
        try:
            ret = yield s.get_next()
            self.assertEqual(ret, None)
            s.close()
        except Exception as e:
            self.assertEqual(e.args[0], 'session or packet error!')
