#! /usr/bin/env python
# coding=utf-8

from pypegasus.pgclient import *
from twisted.trial import unittest
import uuid
import time


class TestBasics(unittest.TestCase):
    TEST_HKEY = 'test_hkey_1'
    TEST_SKEY = 'test_skey_1'
    TEST_VALUE = 'test_value_1'
    count = 10000
    op = ''
    test_data = []
    for i in range(count):
        rand_hkey = uuid.uuid1().hex
        test_data.append((TEST_HKEY + rand_hkey, TEST_SKEY, TEST_VALUE))

    @inlineCallbacks
    def setUp(self):
        self.c = Pegasus(['127.0.0.1:34601', '127.0.0.1:34602', '127.0.0.1:34603'], 'temp')
        ret = yield self.c.init()
        self.assertTrue(ret)

    def tearDown(self):
        self.c.close()
        cost = self.end - self.begin
        print('%s %s cost: %s s, %s s per op \n'
              % (self.count, self.op, cost, cost / self.count))

    @inlineCallbacks
    def test_set_ok(self):
        self.op = 'set'
        self.begin = time.time()
        for hk_sk_v in self.test_data:
            yield self.c.set(hk_sk_v[0], hk_sk_v[1], hk_sk_v[2])
        self.end = time.time()

    @inlineCallbacks
    def test_get_ok(self):
        self.op = 'get'
        self.begin = time.time()
        for hk_sk_v in self.test_data:
            yield self.c.get(hk_sk_v[0], hk_sk_v[1])
        self.end = time.time()

    @inlineCallbacks
    def test_remove_ok(self):
        self.op = 'remove'
        self.begin = time.time()
        for hk_sk_v in self.test_data:
            yield self.c.remove(hk_sk_v[0], hk_sk_v[1])
        self.end = time.time()
