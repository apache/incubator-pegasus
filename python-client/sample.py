#!/usr/bin/env python3
# coding:utf-8

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

from pypegasus.pgclient import Pegasus
from pypegasus.utils.tools import ScanOptions

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred


@inlineCallbacks
def basic_test():
    # init
    c = Pegasus(['127.0.0.1:34601', '127.0.0.1:34602', '127.0.0.1:34603'], 'temp')

    suc = yield c.init()
    if not suc:
        reactor.stop()
        print('ERROR: connect pegasus server failed')
        return

    # set
    try:
        ret = yield c.set('hkey1', 'skey1', 'value', 0, 500)
        print('set ret: ', ret)
    except Exception as e:
        print(e)

    # exist
    ret = yield c.exist('hkey1', 'skey1')
    print('exist ret: ', ret)

    ret = yield c.exist('hkey1', 'skey2')
    print('exist ret: ', ret)

    # get
    ret = yield c.get('hkey1', 'skey1')
    print('get ret: ', ret)

    ret = yield c.get('hkey1', 'skey2')
    print('get ret: ', ret)

    # ttl
    yield c.set('hkey2', 'skey1', 'value', 123)

    d = Deferred()
    reactor.callLater(2, d.callback, 'ok')      # 2 seconds later
    yield d

    ret = yield c.ttl('hkey2', 'skey1')
    print('ttl ret: ', ret)

    # remove
    ret = yield c.remove('hkey2', 'skey1')
    print('remove ret: ', ret)

    # multi_set
    kvs = {'skey1': 'value1', 'skey2': 'value2', 'skey3': 'value3'}
    ret = yield c.multi_set('hkey3', kvs, 999)
    print('multi_set ret: ', ret)

    # multi_get
    ks = set(kvs.keys())
    ret = yield c.multi_get('hkey3', ks)
    print('multi_get ret: ', ret)

    ret = yield c.multi_get('hkey3', ks, 1)
    print('multi_get ret: ', ret)
    while ret[0] == 7:              # has more data
        ks.remove(ret[1].keys()[0])
        ret = yield c.multi_get('hkey3', ks, 1)
        print('multi_get ret: ', ret)

    ret = yield c.multi_get('hkey3', ks, 100, 10000, True)
    print('multi_get ret: ', ret)

    # sort_key_count
    ret = yield c.sort_key_count('hkey3')
    print('sort_key_count ret: ', ret)

    # get_sort_keys
    ret = yield c.get_sort_keys('hkey3', 100, 10000)
    print('get_sort_keys ret: ', ret)

    # multi_del
    ret = yield c.multi_del('hkey3', ks)
    print('multi_del ret: ', ret)

    # scan
    o = ScanOptions()
    o.batch_size = 1
    s = c.get_scanner('hkey3', '1', '7', o)
    while True:
        try:
            ret = yield s.get_next()
            print('get_next ret: ', ret)
        except Exception as e:
            print(e)
            break

        if not ret:
            break
    s.close()

    # scan all
    yield c.multi_set('0', kvs, 999)
    yield c.multi_set('1', kvs, 999)
    yield c.multi_set('2', kvs, 999)
    yield c.multi_set('3', kvs, 999)
    yield c.multi_set('4', kvs, 999)
    yield c.multi_set('5', kvs, 999)
    yield c.multi_set('6', kvs, 999)

    ss = c.get_unordered_scanners(3, o)
    for s in ss:
        while True:
            try:
                ret = yield s.get_next()
                print('get_next ret: ', ret)
            except Exception as e:
                print(e)
                break

            if not ret:
                break
        s.close()

    reactor.stop()


if __name__ == "__main__":
    reactor.callWhenRunning(basic_test)
    reactor.run()
