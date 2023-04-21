#! /usr/bin/env python3
# coding=utf-8

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import uuid
import random
from datetime import time

from twisted.test import proto_helpers
from twisted.trial import unittest

from pypegasus.pgclient import *
from pypegasus.rrdb.ttypes import filter_type
from pypegasus.utils.tools import MultiGetOptions


class TestBasics(unittest.TestCase):
    TEST_HKEY = 'test_hkey_1'
    TEST_SKEY = 'test_skey_1_'
    TEST_VALUE = 'test_value_1_'

    @inlineCallbacks
    def setUp(self):
        self.c = Pegasus(['127.0.0.1:34601', '127.0.0.1:34602', '127.0.0.1:34603'], 'temp')
        ret = yield self.c.init()
        self.assertTrue(ret)

    def tearDown(self):
        self.c.close()

    @inlineCallbacks
    def test_set_ok(self):
        (ret, ign) = yield self.c.set(self.TEST_HKEY, self.TEST_SKEY, self.TEST_VALUE)
        self.assertEqual(ret, error_types.ERR_OK.value)

    # TODO(yingchun): it's flaky, we can fix the test in the future.
    # @inlineCallbacks
    # def test_set_timeout(self):
    #     (ret, ign) = yield self.c.set(self.TEST_HKEY, self.TEST_SKEY, self.TEST_VALUE * 70000, 0, 5)
    #     self.assertEqual(ret, error_types.ERR_TIMEOUT.value)

    @inlineCallbacks
    def test_remove_ok(self):
        (ret, ign) = yield self.c.remove(self.TEST_HKEY, self.TEST_SKEY)
        self.assertEqual(ret, error_types.ERR_OK.value)

        (rc, v) = yield self.c.exist(self.TEST_HKEY, self.TEST_SKEY)
        self.assertEqual(rc, error_types.ERR_OBJECT_NOT_FOUND.value)

    @inlineCallbacks
    def test_exist_ok(self):
        (ret, ign) = yield self.c.set(self.TEST_HKEY, self.TEST_SKEY, self.TEST_VALUE)
        self.assertEqual(ret, error_types.ERR_OK.value)

        (rc, v) = yield self.c.exist(self.TEST_HKEY, self.TEST_SKEY)
        self.assertEqual(rc, error_types.ERR_OK.value)

    @inlineCallbacks
    def test_exist_none(self):
        (ret, ign) = yield self.c.remove(self.TEST_HKEY, self.TEST_SKEY)
        self.assertEqual(ret, error_types.ERR_OK.value)

        (rc, v) = yield self.c.exist(self.TEST_HKEY, self.TEST_SKEY)
        self.assertEqual(rc, error_types.ERR_OBJECT_NOT_FOUND.value)

    @inlineCallbacks
    def test_get_ok(self):
        (ret, ign) = yield self.c.set(self.TEST_HKEY, self.TEST_SKEY, self.TEST_VALUE)
        self.assertEqual(ret, error_types.ERR_OK.value)

        (rc, v) = yield self.c.get(self.TEST_HKEY, self.TEST_SKEY)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(bytes.decode(v), self.TEST_VALUE)

    @inlineCallbacks
    def test_binary_get_ok(self):
        b_hk = b"\x00" + os.urandom(4) + b"\x00"
        b_sk = b"\x00" + os.urandom(4) + b"\x00"
        b_v = b"\x00" + os.urandom(4) + b"\x00"

        (ret, ign) = yield self.c.set(b_hk, b_sk, b_v)
        self.assertEqual(ret, error_types.ERR_OK.value)

        (rc, v) = yield self.c.get(b_hk, b_sk)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(v, b_v)

    @inlineCallbacks
    def test_get_none(self):
        (ret, ign) = yield self.c.remove(self.TEST_HKEY, self.TEST_SKEY)
        self.assertEqual(ret, error_types.ERR_OK.value)

        (rc, v) = yield self.c.get(self.TEST_HKEY, self.TEST_SKEY)
        self.assertEqual(rc, error_types.ERR_OBJECT_NOT_FOUND.value)

    @inlineCallbacks
    def test_ttl_forever(self):
        (ret, ign) = yield self.c.set(self.TEST_HKEY, self.TEST_SKEY, self.TEST_VALUE)
        self.assertEqual(ret, error_types.ERR_OK.value)

        (rc, v) = yield self.c.ttl(self.TEST_HKEY, self.TEST_SKEY)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(v, -1)

    @inlineCallbacks
    def test_ttl_N(self):
        ttl = 60
        (ret, ign) = yield self.c.set(self.TEST_HKEY, self.TEST_SKEY, self.TEST_VALUE, ttl)
        self.assertEqual(ret, error_types.ERR_OK.value)

        (rc, v) = yield self.c.ttl(self.TEST_HKEY, self.TEST_SKEY)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(v, ttl)

    @inlineCallbacks
    def test_ttl_N_with_phase(self):
        ttl = 10
        (ret, ign) = yield self.c.set(self.TEST_HKEY, self.TEST_SKEY, self.TEST_VALUE, ttl)
        self.assertEqual(ret, error_types.ERR_OK.value)

        period = 2
        d = defer.Deferred()
        reactor.callLater(period, d.callback, 'ok')
        yield d

        (rc, v) = yield self.c.ttl(self.TEST_HKEY, self.TEST_SKEY)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(v, ttl - period)

    @inlineCallbacks
    def test_ttl_expired(self):
        ttl = 1
        (ret, ign) = yield self.c.set(self.TEST_HKEY, self.TEST_SKEY, self.TEST_VALUE, ttl)
        self.assertEqual(ret, error_types.ERR_OK.value)

        period = 1.5
        d = defer.Deferred()
        reactor.callLater(period, d.callback, 'ok')
        yield d

        (rc, v) = yield self.c.ttl(self.TEST_HKEY, self.TEST_SKEY)
        self.assertEqual(rc, error_types.ERR_OBJECT_NOT_FOUND.value)

    @inlineCallbacks
    def test_multi_set_ok(self):
        count = 50
        kvs = {self.TEST_SKEY + str(x): self.TEST_VALUE + str(x) for x in range(count)}
        (ret, ign) = yield self.c.multi_set(self.TEST_HKEY, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)

    @inlineCallbacks
    def test_multi_get_ok(self):
        count = 50
        ks = {self.TEST_SKEY + str(x) for x in range(count)}
        kvs = {self.TEST_SKEY + str(x): self.TEST_VALUE + str(x) for x in range(count)}

        (ret, ign) = yield self.c.multi_set(self.TEST_HKEY, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)

        (rc, get_kvs) = yield self.c.multi_get(self.TEST_HKEY, ks)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_kvs), len(kvs))
        self.assertEqual(bytesmap_to_strmap(get_kvs), kvs)

    @inlineCallbacks
    def test_multi_get_opt_ok(self):
        count = 50
        rand_key = uuid.uuid1().hex
        kvs = {self.TEST_SKEY + rand_key + "_" + str(x): self.TEST_VALUE + str(x) for x in range(count)}

        (ret, ign) = yield self.c.multi_set(self.TEST_HKEY, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)

        opt = MultiGetOptions()
        (rc, get_kvs) = yield self.c.multi_get_opt(self.TEST_HKEY,
                                                   self.TEST_SKEY + rand_key + '_0',
                                                   self.TEST_SKEY + rand_key + '_' + '9' * len(str(count)),
                                                   opt,
                                                   500)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_kvs), len(kvs))
        self.assertEqual(bytesmap_to_strmap(get_kvs), kvs)

    @inlineCallbacks
    def test_multi_get_opt_prefix_ok(self):
        count = 50
        rand_key = uuid.uuid1().hex
        rand_key2 = uuid.uuid4().hex
        kvs = {self.TEST_SKEY + rand_key + "_" + str(x): self.TEST_VALUE + str(x) for x in range(count)}
        kvs2 = {self.TEST_SKEY + rand_key2 + "_" + str(x): self.TEST_VALUE + str(x) for x in range(count)}

        (ret, ign) = yield self.c.multi_set(self.TEST_HKEY, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)
        (ret, ign) = yield self.c.multi_set(self.TEST_HKEY, kvs2)
        self.assertEqual(ret, error_types.ERR_OK.value)

        opt = MultiGetOptions()
        opt.sortkey_filter_type = filter_type.FT_MATCH_PREFIX
        opt.sortkey_filter_pattern = self.TEST_SKEY + rand_key
        (rc, get_kvs) = yield self.c.multi_get_opt(self.TEST_HKEY,
                                                   '',
                                                   '',
                                                   opt,
                                                   500)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_kvs), len(kvs))
        self.assertEqual(bytesmap_to_strmap(get_kvs), kvs)

    @inlineCallbacks
    def test_multi_get_opt_postfix_ok(self):
        count = 50
        rand_key = uuid.uuid1().hex
        rand_key2 = uuid.uuid4().hex
        rand_hkey = self.TEST_HKEY + str(random.randint(0, 99))
        kvs = {self.TEST_SKEY + str(x) + "_" + rand_key: self.TEST_VALUE + str(x) for x in range(count)}
        kvs2 = {self.TEST_SKEY + str(x) + "_" + rand_key2: self.TEST_VALUE + str(x) for x in range(count)}

        (ret, ign) = yield self.c.multi_set(rand_hkey, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)
        (ret, ign) = yield self.c.multi_set(rand_hkey, kvs2)
        self.assertEqual(ret, error_types.ERR_OK.value)

        opt = MultiGetOptions()
        opt.sortkey_filter_type = filter_type.FT_MATCH_POSTFIX
        opt.sortkey_filter_pattern = rand_key
        (rc, get_kvs) = yield self.c.multi_get_opt(rand_hkey,
                                                   '',
                                                   '',
                                                   opt,
                                                   500)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_kvs), len(kvs))
        self.assertEqual(bytesmap_to_strmap(get_kvs), kvs)

    @inlineCallbacks
    def test_multi_get_opt_anywhere_ok(self):
        count = 50
        rand_key = uuid.uuid1().hex
        rand_key2 = uuid.uuid4().hex
        rand_hkey = self.TEST_HKEY + str(random.randint(0, 99))
        kvs = {self.TEST_SKEY + str(x) + "_" + rand_key + "_" + str(x): self.TEST_VALUE + str(x) for x in range(count)}
        kvs2 = {self.TEST_SKEY + str(x) + "_" + rand_key2 + "_" + str(x): self.TEST_VALUE + str(x) for x in
                range(count)}

        (ret, ign) = yield self.c.multi_set(rand_hkey, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)
        (ret, ign) = yield self.c.multi_set(rand_hkey, kvs2)
        self.assertEqual(ret, error_types.ERR_OK.value)

        opt = MultiGetOptions()
        opt.sortkey_filter_type = filter_type.FT_MATCH_ANYWHERE
        opt.sortkey_filter_pattern = rand_key
        (rc, get_kvs) = yield self.c.multi_get_opt(rand_hkey,
                                                   '',
                                                   '',
                                                   opt,
                                                   500)
        get_kvs = bytesmap_to_strmap(get_kvs)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_kvs), len(kvs))
        self.assertEqual(get_kvs, kvs)

    @inlineCallbacks
    def test_multi_get_opt_sortkey_inclusive_ok(self):
        rand_key = uuid.uuid1().hex
        start_key = self.TEST_SKEY + rand_key + '_start'
        stop_key = self.TEST_SKEY + rand_key + '_stop'
        start_value = self.TEST_VALUE + 'start'
        stop_value = self.TEST_VALUE + 'stop'
        kvs = {start_key: start_value,
               stop_key: stop_value
               }

        (ret, ign) = yield self.c.multi_set(self.TEST_HKEY, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)

        opt = MultiGetOptions()

        # False False
        opt.start_inclusive = False
        opt.stop_inclusive = False
        (rc, get_kvs) = yield self.c.multi_get_opt(self.TEST_HKEY,
                                                   start_key,
                                                   stop_key,
                                                   opt,
                                                   500)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_kvs), 0)

        # False True
        opt.start_inclusive = False
        opt.stop_inclusive = True
        (rc, get_kvs) = yield self.c.multi_get_opt(self.TEST_HKEY,
                                                   start_key,
                                                   stop_key,
                                                   opt,
                                                   500)
        get_kvs = bytesmap_to_strmap(get_kvs)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_kvs), 1)
        self.assertTrue(stop_key in get_kvs)
        self.assertEqual(get_kvs[stop_key], stop_value)

        # True False
        opt.start_inclusive = True
        opt.stop_inclusive = False
        (rc, get_kvs) = yield self.c.multi_get_opt(self.TEST_HKEY,
                                                   start_key,
                                                   stop_key,
                                                   opt,
                                                   500)
        get_kvs = bytesmap_to_strmap(get_kvs)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_kvs), 1)
        self.assertTrue(start_key in get_kvs)
        self.assertEqual(get_kvs[start_key], start_value)

        # True True
        opt.start_inclusive = True
        opt.stop_inclusive = True
        (rc, get_kvs) = yield self.c.multi_get_opt(self.TEST_HKEY,
                                                   start_key,
                                                   stop_key,
                                                   opt,
                                                   500)
        get_kvs = bytesmap_to_strmap(get_kvs)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_kvs), 2)
        self.assertTrue(start_key in get_kvs)
        self.assertEqual(get_kvs[start_key], start_value)
        self.assertTrue(stop_key in get_kvs)
        self.assertEqual(get_kvs[stop_key], stop_value)

    @inlineCallbacks
    def test_multi_get_opt_no_value_ok(self):
        count = 50
        rand_key = uuid.uuid1().hex
        kvs = {self.TEST_SKEY + rand_key + "_" + str(x): self.TEST_VALUE + str(x) for x in range(count)}

        (ret, ign) = yield self.c.multi_set(self.TEST_HKEY, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)

        opt = MultiGetOptions()
        opt.no_value = True
        (rc, get_kvs) = yield self.c.multi_get_opt(self.TEST_HKEY,
                                                   self.TEST_SKEY + rand_key + '_0',
                                                   self.TEST_SKEY + rand_key + '_' + '9' * len(str(count)),
                                                   opt,
                                                   500)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_kvs), len(kvs))
        for k in kvs.keys():
            self.assertTrue(k in bytesmap_to_strmap(get_kvs))

    @inlineCallbacks
    def test_multi_get_opt_reverse_ok(self):
        count = 50
        steps = 5
        rand_hkey = self.TEST_HKEY + str(random.randint(0, 99))
        self.assertTrue(count % steps == 0)
        split_count = int(count / 5)
        rand_key = uuid.uuid1().hex
        ks = sorted({self.TEST_SKEY + rand_key + "_" + str(x) for x in range(count)})
        kvs = {self.TEST_SKEY + rand_key + "_" + str(x): self.TEST_VALUE + str(x) for x in range(count)}

        (ret, ign) = yield self.c.multi_set(rand_hkey, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)

        stop_key = self.TEST_SKEY + rand_key + '_' + '9' * len(str(count))
        opt = MultiGetOptions()
        opt.reverse = True
        for step in range(steps):
            (rc, get_kvs) = yield self.c.multi_get_opt(rand_hkey,
                                                       self.TEST_SKEY + rand_key + '_0',
                                                       stop_key,
                                                       opt,
                                                       split_count)
            if step == steps - 1:
                self.assertEqual(rc, error_types.ERR_OK.value)
            else:
                self.assertEqual(rc, error_types.ERR_INCOMPLETE_DATA.value)
            self.assertEqual(len(get_kvs), split_count)
            get_kvs = bytesmap_to_strmap(get_kvs)
            for _ in range(split_count):
                k = ks.pop()
                self.assertTrue(k in get_kvs)
                self.assertEqual(get_kvs[k], kvs[k])
                stop_key = k

    @inlineCallbacks
    def test_multi_get_1by1_ok(self):
        count = 5
        ks = {self.TEST_SKEY + str(x) for x in range(count)}
        kvs = {self.TEST_SKEY + str(x): self.TEST_VALUE + str(x) for x in range(count)}

        (ret, ign) = yield self.c.multi_set(self.TEST_HKEY, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)

        get_count = 0
        while ks:
            (rc, get_kvs) = yield self.c.multi_get(self.TEST_HKEY, ks, 1)
            if rc == error_types.ERR_INCOMPLETE_DATA.value \
                    or rc == error_types.ERR_OK.value:
                for (k, v) in get_kvs.items():
                    get_count += 1
                    self.assertIn(bytes.decode(k), ks)
                    ks.remove(bytes.decode(k))
                    self.assertIn(bytes.decode(k), kvs)
                    self.assertEqual(bytes.decode(v), kvs[bytes.decode(k)])

        self.assertEqual(get_count, len(kvs))

    @inlineCallbacks
    def test_multi_del_ok(self):
        count = 50
        ks = {self.TEST_SKEY + str(x) for x in range(count)}

        (rc, del_count) = yield self.c.multi_del(self.TEST_HKEY, ks)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(del_count, len(ks))

    @inlineCallbacks
    def test_multi_get_part_ok(self):
        count = 50
        ks = {self.TEST_SKEY + str(x) for x in range(int(count / 2))}
        kvs = {self.TEST_SKEY + str(x): self.TEST_VALUE + str(x) for x in range(count)}

        (ret, ign) = yield self.c.multi_set(self.TEST_HKEY, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)

        (rc, get_kvs) = yield self.c.multi_get(self.TEST_HKEY, ks)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_kvs), len(ks))
        for (k, v) in get_kvs.items():
            k = bytes.decode(k)
            v = bytes.decode(v)
            self.assertIn(k, ks)
            self.assertIn(k, kvs)
            self.assertEqual(v, kvs[k])

    @inlineCallbacks
    def test_multi_get_more_ok(self):
        count = 50
        ks = {self.TEST_SKEY + str(x) for x in range(count)}
        kvs = {self.TEST_SKEY + str(x): self.TEST_VALUE + str(x) for x in range(int(count / 2))}

        (rc, del_count) = yield self.c.multi_del(self.TEST_HKEY, ks)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(del_count, len(ks))

        (ret, ign) = yield self.c.multi_set(self.TEST_HKEY, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)

        (rc, get_kvs) = yield self.c.multi_get(self.TEST_HKEY, ks)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_kvs), len(kvs))
        for (k, v) in get_kvs.items():
            self.assertIn(bytes.decode(k), ks)
            self.assertIn(bytes.decode(k), kvs)
            self.assertEqual(bytes.decode(v), kvs[bytes.decode(k)])

    @inlineCallbacks
    def test_sort_key_count_ok(self):
        rand_key = uuid.uuid1().hex
        (rc, count) = yield self.c.sort_key_count(self.TEST_HKEY + rand_key)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_get_sort_keys_none(self):
        rand_key = uuid.uuid1().hex
        (rc, get_ks) = yield self.c.get_sort_keys(self.TEST_HKEY + rand_key)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_ks), 0)

    @inlineCallbacks
    def test_get_sort_keys_ok(self):
        count = 50
        rand_key = uuid.uuid1().hex
        ks = {self.TEST_SKEY + str(x) for x in range(count)}
        kvs = {self.TEST_SKEY + str(x): self.TEST_VALUE + str(x) for x in range(count)}

        (rc, del_count) = yield self.c.multi_del(self.TEST_HKEY + rand_key, ks)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(del_count, len(ks))

        (ret, ign) = yield self.c.multi_set(self.TEST_HKEY + rand_key, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)

        (rc, get_ks) = yield self.c.get_sort_keys(self.TEST_HKEY + rand_key)
        self.assertEqual(rc, error_types.ERR_OK.value)
        self.assertEqual(len(get_ks), count)

    @inlineCallbacks
    def test_scan_none(self):
        rand_key = uuid.uuid1().hex
        o = ScanOptions()
        s = self.c.get_scanner(self.TEST_HKEY + rand_key, '\x00\x00', '\xFF\xFF', o)
        ret = yield s.get_next()
        self.assertEqual(ret, None)
        s.close()

    @inlineCallbacks
    def test_scan_all(self):
        count = 50
        kvs = {self.TEST_SKEY + str(x): self.TEST_VALUE + str(x) for x in range(count)}

        rand_key = uuid.uuid1().hex
        rand_hkey = self.TEST_HKEY + str(random.randint(0, 99))
        (ret, ign) = yield self.c.multi_set(rand_hkey + rand_key, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)

        o = ScanOptions()
        s = self.c.get_scanner(rand_hkey + rand_key, '\x00\x00', '\xFF\xFF', o)
        get_count = 0
        last_sk = None
        while True:
            hk_sk_v = yield s.get_next()
            if hk_sk_v is None:
                break
            else:
                get_count += 1
                hk = hk_sk_v[0][0]
                sk = hk_sk_v[0][1]
                v = hk_sk_v[1]

                self.assertNotEqual(sk, None)
                if last_sk is not None:
                    self.assertLess(last_sk, sk)
                last_sk = sk

                self.assertEqual(hk, rand_hkey + rand_key)
                self.assertIn(sk, kvs)
                self.assertEqual(bytes.decode(v), kvs[sk])
        s.close()
        self.assertEqual(count, get_count)

    @inlineCallbacks
    def test_scan_part(self):
        count = 50
        self.assertLess(2, count)
        count_len = len(str(count))
        kvs = {self.TEST_SKEY + str(x).zfill(count_len): self.TEST_VALUE + str(x) for x in range(count)}
        sub_kvs = {self.TEST_SKEY + str(x).zfill(count_len): self.TEST_VALUE + str(x) for x in range(1, count - 1)}

        rand_key = uuid.uuid1().hex
        (ret, ign) = yield self.c.multi_set(self.TEST_HKEY + rand_key, kvs)
        self.assertEqual(ret, error_types.ERR_OK.value)

        o = ScanOptions()
        s = self.c.get_scanner(self.TEST_HKEY + rand_key,
                               self.TEST_SKEY + str(1).zfill(count_len),
                               self.TEST_SKEY + str(count - 1).zfill(count_len),
                               o)
        get_count = 0
        last_sk = None
        while True:
            hk_sk_v = yield s.get_next()
            if hk_sk_v is None:
                break
            else:
                get_count += 1
                hk = hk_sk_v[0][0]
                sk = hk_sk_v[0][1]
                v = hk_sk_v[1]

                self.assertNotEqual(sk, None)
                if last_sk is not None:
                    self.assertLess(last_sk, sk)
                last_sk = sk

                self.assertEqual(hk, self.TEST_HKEY + rand_key)
                self.assertIn(sk, sub_kvs)
                self.assertEqual(bytes.decode(v), sub_kvs[sk])
        s.close()
        self.assertEqual(len(sub_kvs), get_count)

    @inlineCallbacks
    def test_unordered_scan_none(self):
        split_count = 5
        o = ScanOptions()
        scanners = self.c.get_unordered_scanners(split_count, o)
        self.assertEqual(len(scanners), split_count)
        for scanner in scanners:
            while True:
                hk_sk_v = yield scanner.get_next()
                # self.assertEqual(hk_sk_v, None)                   # TODO there maybe some remain data
                if hk_sk_v is None:
                    break
            scanner.close()

    @inlineCallbacks
    def test_unordered_scan_all(self):
        hkey_count = 20
        count = 50
        rand_key = uuid.uuid1().hex
        hks = {self.TEST_HKEY + rand_key + '_' + str(i) for i in range(hkey_count)}
        kvs = {self.TEST_SKEY + str(x): self.TEST_VALUE + str(x) for x in range(count)}

        for hk in hks:
            (ret, ign) = yield self.c.multi_set(hk, kvs)
            self.assertEqual(ret, error_types.ERR_OK.value)

        split_count = 5
        o = ScanOptions()
        scanners = self.c.get_unordered_scanners(split_count, o)
        self.assertEqual(len(scanners), split_count)

        get_hk_count = 0
        for scanner in scanners:
            last_hk = None
            last_sk = None
            hk_found = False
            get_count = 0
            while True:
                hk_sk_v = yield scanner.get_next()
                if hk_sk_v is None:
                    break

                hk = hk_sk_v[0][0]
                sk = hk_sk_v[0][1]
                v = hk_sk_v[1]

                self.assertNotEqual(hk, None)
                if last_hk is None:
                    last_hk = hk
                    get_hk_count += 1

                self.assertNotEqual(sk, None)
                if hk == last_hk:
                    get_count += 1
                    if last_sk is not None:
                        self.assertLess(last_sk, sk)
                else:
                    get_hk_count += 1
                    if hk_found:
                        self.assertEqual(count, get_count)
                    get_count = 1
                    last_hk = hk
                last_sk = sk

                if hk in hks:
                    hk_found = True
                    self.assertIn(sk, kvs)
                    self.assertEqual(bytes.decode(v), kvs[sk])
                else:
                    hk_found = False

            scanner.close()

        self.assertLessEqual(hkey_count, get_hk_count)


def bytesmap_to_strmap(bm):
    sm = {}
    for k, v in bm.items():
        sm[bytes.decode(k)] = bytes.decode(v)
    return sm
