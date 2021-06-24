#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import redis

host = "127.0.0.1"
port = 6379
c = redis.Redis(host=host, port=port)

print "check begin"
#append
c.execute_command("del", "mykey")
ret = c.execute_command("append", "mykey", "Hello")
if ret != 5:
    print "append"+" ret not compatible " + "redis: 5" + " checker: " + str(ret)
ret = c.execute_command("append", "mykey", " World")
ret = c.execute_command("get", "mykey")
if ret != "Hello World":
    print "append"+" result wrong " + "redis: \"Hello World\"" + " checker: " + str(ret)

#bitcount
c.execute_command("del", "mykey")
ret = c.execute_command("set", "mykey", "foobar")
ret = c.execute_command("bitcount", "mykey")
if ret != 26:
    print "bitcount"+" result wrong " + "redis: 26" + " checker: " + str(ret)
ret = c.execute_command("bitcount", "mykey", 0, 0)
if ret != 4:
    print "bitcount"+" result wrong " + "redis: 4" + " checker: " + str(ret)
ret = c.execute_command("bitcount", "mykey", 1, 1)
if ret != 6:
    print "bitcount"+" result wrong " + "redis: 6" + " checker: " + str(ret)

#bitop
c.execute_command("del", "key1")
c.execute_command("del", "key2")
c.execute_command("del", "dest")
c.execute_command("set", "key1", "foobar")
c.execute_command("set", "key2", "abcdef")
ret = c.bitop("and","dest", "key1", "key2")
if ret != 6:
    print "bitop"+" ret not compatible " + "redis: 6" + " checker: " + str(ret)
ret = c.get("dest")
if ret != "`bc`ab":
    print "bitop"+" result wrong " + "redis: \"`bc`ab\"" + " checker: " + ret

#bitpos
try:
    c.delete("mykey")
    c.set("mykey","\xff\xf0\x00")
    ret = c.bitpos("mykey",0)
    if ret != 12:
        print "bitpos" + " result wrong " + "redis: 12" + " checker: " + str(ret)
except Exception as e:
    print "bitop Exception: " + str(e)

#decr
c.delete("mykey")
c.set("mykey",10)
ret = c.execute_command("decr","mykey")
if ret != 9:
    print "decr" + " ret not compatible " + "redis: 9" + " checker: " + str(ret)


#decrby
c.delete("mykey")
c.set("mykey",10)
ret = c.execute_command("decrby","mykey",3)
if ret != 7:
    print "decr" + " ret not compatible " + "redis: 7" + " checker: " + str(ret)

#getbit
c.delete("mykey")
c.execute_command("setbit","mykey",7,1)
ret = c.execute_command("getbit", "mykey", 0)
if ret != 0:
    print "getbit"+" result wrong " + "redis: 0" + " checker: " + str(ret)
ret = c.execute_command("getbit", "mykey", 7)
if ret != 1:
    print "getbit"+" result wrong " + "redis: 1" + " checker: " + str(ret)

#getrange
c.delete("mykey")
c.set("mykey", "This is a string")
ret = c.execute_command("getrange", "mykey", 0, 3)
if ret != "This":
    print "getrange"+" result wrong "+"redis: \"This\""+" checker: "+str(ret)
ret = c.execute_command("getrange", "mykey", -3, -1)
if ret != "ing":
    print "getrange"+" result wrong "+"redis: \"ing\""+" checker: "+str(ret)
ret = c.execute_command("getrange", "mykey", 0, -1)
if ret != "This is a string":
    print "getrange"+" result wrong "+"redis: \"This is a string\""+" checker: "+str(ret)
ret = c.execute_command("getrange", "mykey", 10, 100)
if ret != "string":
    print "getrange"+" result wrong "+"redis: \"string\""+" checker: "+str(ret)


#getset
c.delete("mycounter")
c.execute_command("incr", "mycounter")
ret = c.execute_command("getset", "mycounter", 0)
if str(ret) != "1":
    print "getset" + " result wrong " + "redis: 1" + " checker: "+str(ret)

#incr
c.set("mykey", 10)
ret = c.execute_command("incr", "mykey")
if str(ret) != "11":
    print "incr" + " ret not compatible " + "redis: 11" + " checker: "+str(ret)
ret = c.get("mykey")
if str(ret) != "11":
    print "incr" + " result wrong " + "redis: 11" + " checker: " + ret

#incrby
c.set("mykey", 10)
ret = c.execute_command("incrby", "mykey", 5)
if str(ret) != "15":
    print "incrby" + " ret not compatible " + "redis: 15" + " checker: "+str(ret)
ret = c.get("mykey")
if str(ret) != "15":
    print "incrby" + " result wrong " + "redis: 15" + " checker: " + str(ret)


#incrbyfloat
c.set("mykey", 10.50)
ret = c.execute_command("incrbyfloat", "mykey", 0.1)
if ret != "10.6":
    print "incrbyfloat" + " ret not compatible " + "redis: 10.6" + " checker: "+str(ret)
ret = c.get("mykey")
if ret != "10.6":
    print "incrbyfloat" + " result wrong " + "redis: 10.6" + " checker: " + ret

#mget
c.set("key1", "Hello")
c.set("key2", "World")
r1,r2,r3 = c.execute_command("mget", "key1", "key2", "nonexisting")
if r1 != "Hello" or r2 != "World" or r3 != None:
    print "mget" + " result wrong " + "redis: Hello World None" + " checker: " + r1+ r2 + r3

#mset
c.execute_command("mset", "key1", "Hello", "key2", "World")
r1 = c.get("key1")
if r1 != "Hello":
    print "mset" + " result wrong " + "redis: Hello" + " checker: "+ str(ret)
r2 = c.get("key2")
if r2 != "World":
    print "mset" + " result wrong " + "redis: World" + " checker: "+ str(ret)

#msetnx
c.delete("key1")
c.delete("key2")
ret = c.execute_command("msetnx", "key1", "Hello", "key2", "there")
if str(ret) != "1":
    print "msetnx" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.execute_command("msetnx", "key2", "there", "key3", "world")
if str(ret) != "0":
    print "msetnx" + " ret not compatible " + "redis: 0" + " checker: " + str(ret)
r1,r2,r3 = c.execute_command("mget", "key1", "key2", "key3")
if r1 != "Hello" or r2 !="there" or r3 != None:
    print "msetnx" + " result wrong " + "redis: Hello there None"+ " Checker: " + str(r1) + " " + " " + str(r2) + " " + str(r3)

#setbit
c.delete("mykey")
ret = c.execute_command("setbit", "mykey", 7, 1)
if ret != 0:
    print "setbit" + " ret not compatible " + "redis: 0" + " checker: " + str(ret)
ret = c.execute_command("setbit", "mykey", 7, 0)
if ret != 1:
    print "setbit" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.get("mykey").encode('hex')
if str(ret) != "00":
    print "setbit" + " result wrong " + "redis: 00" + " checker: " + str(ret)

#setex
c.delete("mykey")
ret = c.execute_command("setex", "mykey", 10, "hello")
if ret != "OK":
    print "setex" + " ret not compatible " + "redis: OK" + " checker: "+str(ret)
ret = c.ttl("mykey")
if ret != 10:
    print "setex" + " result wrong " + "redis: 10" + " checker: "+str(ret)

#setnx
c.delete("mykey")
ret = c.execute_command("setnx", "mykey", "Hello")
if ret != 1:
    print "setnx" + " ret not compatible " + "redis: 1" + " checker: "+str(ret)

ret = c.execute_command("setnx", "mykey", "Hello")
if ret != 0:
    print "setnx" + " ret not compatible " + "redis: 0" + " checker: "+str(ret)

ret = c.get("mykey")
if ret != "Hello":
    print "setnx" + " result wrong " + "redis: Hello" + " checker:  " + str(ret)

#setrange
c.set("key1", "Hello World")
ret = c.execute_command("setrange", "key1", 6, "Redis")
if ret != 11:
    print "setrange" + " ret not compatile " + "redis: 11" + " checker: " + str(ret)

ret = c.get("key1")
if ret != "Hello Redis":
    print "setrange" + " wrong result " + "redis: Hello Redis" + " checker: " + str(ret)

#strlen
try:
    c.delete("mykey")
    c.set("mykey", "Hello world")
    ret = c.execute_command("strlen", "mykey")
    if ret != 11:
        print "strlen" + " wrong result " + "redis: 11" + " checker: " + str(ret)
    ret = c.execute_command("strlen", "nonexisting")
    if ret != 0:
        print "strlen" + " wrong result " + "redis: 0" + " checker: " + str(ret)
except Exception as e:
    print "strlen Exception: " + str(e)


#hset
c.delete("myhash")
ret = c.execute_command("hset", "myhash", "field1", "Hello")
if str(ret) != "1":
    print "hset" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.hget("myhash", "field1")
if str(ret) != "Hello":
    print "hset" + " wrong result " + "redis: Hello" + " checker: " + str(ret)

#hget
c.hset("myhash", "field1", "foo")
ret = c.execute_command("hget", "myhash", "field1")
if str(ret) != "foo":
    print "hget" + " result wrong " + "redis: foo" + " checker: " + str(ret)
ret = c.execute_command("hget", "myhash", "field2")
if ret != None:
    print "hget" + " result wrong " + "redis: None" + " checker: " + str(ret)

#hdel
c.hset("myhash", "field1", "foo")
ret = c.execute_command("hdel", "myhash", "field1")
if str(ret) != "1":
    print "hdel" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.execute_command("hdel", "myhash", "field2")
if str(ret) != "0":
    print "hdel" + " ret not compatible " + "redis: 0" + " checker: " + str(ret)
ret = c.execute_command("hdel", "myhash", "field1")
if str(ret) != "0":
    print "hdel" + " ret not compatible " + "redis: 0" + " checker: " + str(ret)

#hexists
c.delete("myhash")
c.hset("myhash", "field1", "foo")
ret = c.execute_command("hexists", "myhash", "field1")
if str(ret) != "1":
    print "hexists" + " wrong result " + "redis: 1" + " checker: " + str(ret)
ret = c.execute_command("hexists", "myhash", "field2")
if str(ret) != "0":
    print "hexists" + " wrong result " + "redis: 0" + " checker: " + str(ret)

#hgetall
c.delete("myhash")
c.hset("myhash", "field1", "Hello")
c.hset("myhash", "field2", "World")
ret = c.execute_command("hgetall", "myhash")
if ret != ['field1', 'Hello', 'field2', 'World']:
    print "hgetall" + " wrong result " + "redis: ['field1', 'Hello', 'field2', 'World']" + " checker: " + str(ret)

#hincrby
c.delete("myhash")
c.hset("myhash", "field", 5)
ret = c.execute_command("hincrby", "myhash", "field", 1)
if str(ret) != "6":
    print "hincrby" + " ret not compatible " + "redis: 6 " + " checker: " + str(ret)
ret = c.hget("myhash", "field")
if str(ret)!="6":
    print "hincrby" + " result wrong " + "redis: 6" + " checker: " + str(ret)

#hincrbyfloat
c.delete("mykey")
c.hset("mykey", "field", 10.50)
ret = c.execute_command("hincrbyfloat", "mykey", "field", 0.1)
if str(ret)!="10.6":
    print "hincrbyfloat" + " ret not compatible " + "redis: 10.6" + " checker: " + str(ret)
ret = c.execute_command("hincrbyfloat", "mykey", "field", -5)
if str(ret)!="5.6":
    print "hincrbyfloat" + " ret not compatible " + "redis: 5.6" + " checker: " + str(ret)

#hkeys
c.delete("myhash")
c.hset("myhash", "field1", "Hello")
c.hset("myhash", "field2", "World")
ret = c.execute_command("hkeys", "myhash")
if ret != ['field1', 'field2']:
    print "hkeys" + " result wrong " + "redis: ['field1', 'field2']" + " checker: " + str(ret)

#hlen
c.delete("myhash")
c.hset("myhash", "field1", "Hello")
c.hset("myhash", "field2", "World")
ret = c.execute_command("hlen", "myhash")
if str(ret) != "2":
    print "hlen" + " result wrong " + "redis: 2" + " checker: " + str(ret)

#hmget
c.delete("myhash")
c.hset("myhash", "field1", "Hello")
c.hset("myhash", "field2", "World")
ret = c.execute_command("hmget", "myhash", "field1", "field2", "nofield")
if ret != ['Hello', 'World', None]:
    print "hmget" + " result wrong " + "redis: ['Hello', 'World', None]" + str(ret)

#hmset
c.delete("myhash")
ret = c.execute_command("hmset", "myhash", "field1", "Hello", "field2", "World")
if str(ret) != "OK":
    print "hmset" + " ret not compatible " + "redis: OK" + "checker: " + str(ret)
ret = c.hget("myhash", "field1")
if str(ret) != "Hello":
        print "hmset" + " result wrong " + "redis: Hello" + " checker: " + str(ret)
ret = c.hget("myhash", "field2")
if str(ret) != "World":
        print "hmset" + " result wrong " + "redis: Hello" + " checker: " + str(ret)

#hsetnx
c.delete("myhash")
ret = c.execute_command("hsetnx", "myhash", "field", "Hello")
if str(ret) != "1":
    print "hsetnx" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.execute_command("hsetnx", "myhash", "field", "Hello")
if str(ret) != "0":
    print "hsetnx" + " ret not compatible " + "redis: 0" + " checker: " + str(ret)
ret = c.hget("myhash", "field")
if str(ret) != "Hello":
    print "hsetnx" + " result wrong " + "redis: Hello" + " checker: " + str(ret)

#hstrlen
c.delete("myhash")
c.execute_command("hmset", "myhash", "f1", "HelloWorld", "f2", 99, "f3", -256)
try:
    ret = c.execute_command("hstrlen", "myhash", "f1")
    if str(ret) != "10":
        print "hstrlen" + " result wrong " + "redis: 10" + " checker: " + str(ret)
    ret = c.execute_command("hstrlen", "myhash", "f2")
    if str(ret) != "2":
        print "hstrlen" + " result wrong " + "redis: 2" + " checker: " + str(ret)
    ret = c.execute_command("hstrlen", "myhash", "f3")
    if str(ret) != "4":
        print "hstrlen" + " result wrong " + "redis: 4" + " checker: " + str(ret)
except Exception as e:
    print "hstrlen Exception " + str(e)

#hvals
c.delete("myhash")
c.hset("myhash", "field1", "Hello")
c.hset("myhash", "field2", "World")
ret = c.execute_command("hvals", "myhash")
if ret != ['Hello', 'World']:
    print "hvals" + " result wrong " + "redis: ['Hello', 'World']" + " checker: " + str(ret)

#hscan
c.delete("myhash")
c.hset("myhash", "field1", "Hello")
c.hset("myhash", "field2", "World")
for v in c.hscan_iter("myhash"):
    print v

#lpush
c.delete("mylist")
ret = c.execute_command("lpush", "mylist", "world")
if str(ret) != "1":
    print "lpush" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.execute_command("lpush", "mylist", "hello")
if str(ret) != "2":
    print "lpush" + " ret not compatible " + "redis: 2" + " checker: " + str(ret)
ret = c.lrange("mylist", 0, -1)
if ret != ['hello', 'world']:
    print "lpush" + " result wrong " + "redis: ['hello', 'world']" + " checker: " + str(ret)

#lindex
c.delete("mylist")
c.lpush("mylist", "World")
c.lpush("mylist", "Hello")
ret = c.execute_command("lindex", "mylist", 0)
if str(ret) != "Hello":
    print "lindex" + " result wrong " + "redis: Hello" + " checker: " + str(ret)
ret = c.execute_command("lindex", "mylist", -1)
if str(ret) != "World":
    print "lindex" + " result wrong " + "redis: World" + " checker: " + str(ret)
ret = c.execute_command("lindex", "mylist", 3)
if ret != None:
    print "lindex" + " result wrong " + "redis: None" + " checker: " + str(ret)

#rpush
c.delete("mylist")
ret = c.execute_command("rpush", "mylist", "hello")
if str(ret) != "1":
    print "rpush" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.execute_command("rpush", "mylist", "world")
if str(ret) != "2":
    print "rpush" + " ret not compatible " + "redis: 2" + " checker: " + str(ret)
ret = c.lrange("mylist", 0, -1)
if ret != ['hello', 'world']:
    print "rpush" + " result wrong " + "redis: ['hello', 'world']" + " checker: " + str(ret)


#linsert
c.delete("mylist")
c.rpush("mylist", "Hello")
c.rpush("mylist", "World")
ret = c.execute_command("linsert", "mylist", "before", "World", "There")
if str(ret) != "3":
    print "linsert"  + " ret not compatible " + "redis: 3" + " checker: " + str(ret)
ret = c.lrange("mylist", 0, -1)
if ret != ['Hello', 'There', 'World']:
    print "linsert" + " result wrong " + "redis: ['Hello', 'There', 'World']" + " checker: " + str(ret)

#llen
c.delete("mylist")
c.lpush("mylist", "World")
c.lpush("mylist", "Hello")
ret = c.execute_command("llen","mylist")
if str(ret) != "2":
    print "llen" + " result wrong " + "redis: 2" + " checker: " + str(ret)

#lpop
c.delete("mylist")
c.rpush("mylist", "one")
c.rpush("mylist", "two")
c.rpush("mylist", "three")
ret = c.lpop("mylist")
if str(ret) != "one":
    print "lpop" + " result wrong " + "redis: one" + " checker: " + str(ret)
ret = c.lrange("mylist", 0, -1)
if ret != ['two', 'three']:
    print "lpop" + " result wrong " + "redis: ['two', 'three']" + " checker: " + str(ret)

#lpushx
c.delete("mylist")
c.delete("myotherlist")
c.lpush("mylist", "World")
c.lpush("mylist", "Hello")
c.lpushx("myotherlist", "Hello")
ret = c.lrange("mylist", 0, -1)
ret = c.lrange("myotherlist", 0, -1)
if ret != []:
    print "lpushx" + " result wrong " + "redis: []" + " checker: " + str(ret)

#lrem
c.delete("mylist")
c.rpush("mylist", "hello")
c.rpush("mylist", "hello")
c.rpush("mylist", "foo")
c.rpush("mylist", "hello")
ret = c.execute_command("lrem", "mylist", -2, "hello")
if str(ret) != "2":
    print "lrem" + " ret not compabile " + "redis: 2" + " checker: " + str(ret)
ret = c.lrange("mylist", 0, -1)
if ret != ['hello', 'foo']:
    print "lrem" + " result wrong " + "redis: ['hello', 'foo']" + " checker: " + str(ret)

#lset
c.delete("mylist")
c.rpush("mylist", "one")
c.rpush("mylist", "two")
c.rpush("mylist", "three")
ret = c.execute_command("lset", "mylist", 0, "four")
if str(ret) != "OK":
    print "lset" + " ret not compatible " + "redis: OK" + " checker: " + str(ret)
ret = c.execute_command("lset", "mylist", -2, "five")
if str(ret) != "OK":
    print "lset" + " ret not compatible " + "redis: OK" + " checker: " + str(ret)
ret = c.lrange("mylist", 0, -1)
if ret != ['four', 'five', 'three']:
    print "lset" + " result wrong " + "redis: ['four', 'five', 'three']" + " checker: " + str(ret)

#ltrim
c.delete("mylist")
c.rpush("mylist", "one")
c.rpush("mylist", "two")
c.rpush("mylist", "three")
ret = c.execute_command("ltrim", "mylist", 1, -1)
if str(ret) != "OK":
    print "ltrim" + " ret not compabile " + "redis: OK" + " checker: "  + str(ret)
ret = c.lrange("mylist", 0, -1)
if ret != ['two', 'three']:
    print "ltrim" + " result wrong " + "redis: ['two', 'three']" + " checker: " + str(ret)

#rpop
c.delete("mylist")
c.rpush("mylist", "one")
c.rpush("mylist", "two")
c.rpush("mylist", "three")
ret = c.execute_command("rpop", "mylist")
if str(ret) != "three":
    print "rpop" + " result wrong " + "redis: three" + " checker: " + str(ret)

#rpoplpush
c.delete("mylist")
c.delete("myotherlist")
c.rpush("mylist", "one")
c.rpush("mylist", "two")
c.rpush("mylist", "three")
ret = c.execute_command("rpoplpush", "mylist", "myotherlist")
if str(ret) != "three":
    print "rpoplpush" + " ret not compatible " + "redis: three" + " checker: " + str(ret)
ret = c.lrange("mylist", 0, -1)
if ret != ['one', 'two']:
    print "rpoplpush" + " result wrong " + "redis: ['one', 'two']" + " checker: " + str(ret)
ret = c.lrange("myotherlist", 0, -1)
if ret != ['three']:
    print "rpoplpush" + " result wrong " + "redis: ['three']" + " checker: " + str(ret)

#rpushx
c.delete("mylist")
c.delete("myotherlist")
c.rpush("mylist", "Hello")
ret = c.execute_command("rpushx", "mylist", "World")
if str(ret) != "2":
    print "rpushx" + " ret not compatible " + "redis: 2" + " checker: " + str(ret)
ret = c.execute_command("rpushx", "myotherlist", "World")
if str(ret) != "0":
    print "rpushx" + " ret not compatible " + "redis: 0" + " checker: " + str(ret)
ret = c.lrange("mylist", 0, -1)
if ret != ['Hello', 'World']:
    print "rpushx" + " result wrong " + "redis: ['Hello', 'World']" + str(ret)
ret = c.lrange("myotherlist", 0, -1)
if ret != []:
    print "rpushx" + " result wrong " + "redis: []" + str(ret)

#sadd
c.delete("myset")
ret = c.execute_command("sadd", "myset", "Hello")
if str(ret) != "1":
    print "sadd" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.execute_command("sadd", "myset", "World")
if str(ret) != "1":
    print "sadd" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.execute_command("sadd", "myset", "World")
if str(ret) != "0":
    print "sadd" + " ret not compatible " + "redis: 0" + " checker: " + str(ret)
ret = c.smembers("myset")
if ret != set(['World', 'Hello']):
    print "sadd" + " result wrong " + "redis: set(['World', 'Hello'])" + str(ret)

#scard
c.delete("myset")
c.sadd("myset", "Hello")
c.sadd("myset", "World")
ret = c.scard("myset")
if str(ret) != "2":
    print "scard" + " result wrong " + "redis: 2" + " checker: " + str(ret)

#sdiff
c.delete("key1")
c.delete("key2")
c.sadd("key1", "a")
c.sadd("key1", "b")
c.sadd("key1", "c")
c.sadd("key2", "c")
c.sadd("key2", "d")
c.sadd("key2", "e")
ret = c.execute_command("sdiff", "key1", "key2")
if ret != ['a', 'b']:
    print "sdiff" + " result wrong " + "redis: ['a', 'b']" + " checker: " + str(ret)

#sdiffstore
c.delete("key1")
c.delete("key2")
c.delete("key")
c.sadd("key1", "a")
c.sadd("key1", "b")
c.sadd("key1", "c")
c.sadd("key2", "c")
c.sadd("key2", "d")
c.sadd("key2", "e")
ret = c.execute_command("sdiffstore", "key", "key1", "key2")
if str(ret) != "2":
    print "sdiffstore" + " ret not compatible " + "redis: 2" + " checker: " + str(ret)
ret = c.smembers("key")
if ret != set(['a', 'b']):
    print "sdiffstore" + " result wrong " + "redis: set(['a', 'b'])" + " checker: " + str(ret)

#sinter
c.delete("key1")
c.delete("key2")
c.sadd("key1", "a")
c.sadd("key1", "b")
c.sadd("key1", "c")
c.sadd("key2", "c")
c.sadd("key2", "d")
c.sadd("key2", "e")
ret = c.execute_command("sinter", "key1", "key2")
if ret != ['c']:
    print "sinter" + " result wrong " + "redis: ['c']" + " checker: " + str(ret)

#sinterstore
c.delete("key1")
c.delete("key2")
c.delete("key")
c.sadd("key1", "a")
c.sadd("key1", "b")
c.sadd("key1", "c")
c.sadd("key2", "c")
c.sadd("key2", "d")
c.sadd("key2", "e")
ret = c.execute_command("sinterstore", "key", "key1", "key2")
if str(ret) != "1":
    print "sinterstore" + " ret not compatible " + " checker: " + str(ret)
ret = c.smembers("key")
if ret != set(['c']):
    print "sinterstore" + " result wrong " + "redis: set(['c'])" + " checker: " + str(ret)

#sismember
c.delete("myset")
c.sadd("myset", "one")
ret = c.execute_command("sismember", "myset", "one")
if str(ret) != "1":
    print "sismember" + " result wrong " + "redis: 1" + " checker: " + str(ret)
ret = c.execute_command("sismember", "myset", "two")
if str(ret) != "0":
    print "sismember" + " result wrong " + "redis: 0" + " checker: " + str(ret)

#smove
c.delete("myset")
c.delete("myotherset")
c.sadd("myset", "one")
c.sadd("myset", "two")
c.sadd("myotherset", "three")
ret = c.execute_command("smove", "myset", "myotherset", "two")
if str(ret) != "1":
    print "smove" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.smembers("myset")
if ret != set(['one']):
    print "smove" + " result wrong " + "redis: set(['one'])" + " checker: " + str(ret)
ret = c.smembers("myotherset")
if ret != set(['three', 'two']):
    print "smove" + " result wrong " + "redis: set(['three', 'two'])" + " checker: " + str(ret)

#srem
c.delete("myset")
c.sadd("myset", "one")
c.sadd("myset", "two")
c.sadd("myset", "three")
ret = c.execute_command("srem", "myset", "one")
ret = c.execute_command("srem", "myset", "four")
ret = c.smembers("myset")
if ret != set(['three', 'two']):
    print "srem" + " result wrong " + "redis: set(['three', 'two'])" + " checker: " + str(ret)

#sunion
c.delete("key1")
c.delete("key2")
c.sadd("key1", "a")
c.sadd("key1", "b")
c.sadd("key1", "c")
c.sadd("key2", "c")
c.sadd("key2", "d")
c.sadd("key2", "e")
ret = c.execute_command("sunion", "key1", "key2")
if ret != ['a', 'b', 'c', 'd', 'e']:
    print "sunion" + " result wrong " + "redis: ['a', 'b', 'c', 'd', 'e']" + " checker: " + str(ret)

#sunionstore
c.delete("key1")
c.delete("key2")
c.delete("key")
c.sadd("key1", "a")
c.sadd("key1", "b")
c.sadd("key1", "c")
c.sadd("key2", "c")
c.sadd("key2", "d")
c.sadd("key2", "e")
ret = c.execute_command("sunionstore", "key", "key1", "key2")
if str(ret) != "5":
    print "sunionstore" + " ret not compatible " + "redis: 5" + " checker: " + str(ret)

ret = c.smembers("key")
if ret != set(['a', 'c', 'b', 'e', 'd']):
    print "sunionstore" + " result wrong " + "redis: set(['a', 'c', 'b', 'e', 'd'])" + " checker: " + str(ret)

#zadd
c.delete("myzset")
ret = c.execute_command("zadd", "myzset", 1, "one")
if str(ret) != "1":
    print "zadd" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.execute_command("zadd", "myzset", 1, "uno")
if str(ret) != "1":
    print "zadd" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.execute_command("zadd", "myzset", 2, "two", 3, "three")
if str(ret) != "2":
    print "zadd" + " ret not compatible " + "redis: 2" + " checker: " + str(ret)
ret = c.zrange("myzset", 0, -1, withscores=True)
if ret != [('one', 1.0), ('uno', 1.0), ('two', 2.0), ('three', 3.0)]:
    print "zadd" + " result wrong " + "redis: [('one', 1.0), ('uno', 1.0), ('two', 2.0), ('three', 3.0)]" + " checker: " + str(ret)

#zcard
c.delete("myzset")
c.zadd("myzset", "one", 1)
c.zadd("myzset", "two", 2)
ret = c.execute_command("zcard", "myzset")
if str(ret) != "2":
    print "zcard" + " result wrong " + "redis: 2" + " checker: " + str(ret)

#zcount
c.delete("myzset")
c.zadd("myzset", "one", 1)
c.zadd("myzset", "two", 2)
c.zadd("myzset", "three", 3)
ret = c.execute_command("zcount", "myzset", "-inf", "+inf")
if str(ret) != "3":
    print "zcount" + " result wrong " + "redis: 3" + " checker: " + str(ret)
ret = c.execute_command("zcount", "myzset", "(1", "3")
if str(ret) != "2":
    print "zcount" + " result wrong " + "redis: 2" + " checker: " + str(ret)

#zincrby
c.delete("myzset")
c.zadd("myzset", "one", 1)
c.zadd("myzset", "two", 2)
ret = c.execute_command("zincrby", "myzset", 2, "one")
if str(ret) != "3":
    print "zincrby" + " ret not compatible " + "redis: 3" + " checker: " + str(ret)
ret = c.zrange("myzset", 0, -1, withscores=True)
if ret != [('two', 2.0), ('one', 3.0)]:
    print "zincrby" + " result wrong " + "redis: [('two', 2.0), ('one', 3.0)]" + " checker: " + str(ret)

#zinterstore
c.delete("zset1")
c.delete("zset2")
c.delete("out")
c.zadd("zset1", "one", 1)
c.zadd("zset1", "two", 2)
c.zadd("zset2", "one", 1)
c.zadd("zset2", "two", 2)
c.zadd("zset2", "three", 3)
ret = c.execute_command("zinterstore", "out", 2, "zset1", "zset2", "weights", 2, 3)
if str(ret) != "2":
    print "zinterstore" + " ret not compatible " + "redis: 2" + " checker: " + str(ret)
ret = c.zrange("out", 0, -1, withscores=True)
if ret != [('one', 5.0), ('two', 10.0)]:
    print "zinterstore" + " result wrong " + "redis: [('one', 5.0), ('two', 10.0)]" + " checker: " + str(ret)

#zlexcount
c.delete("myzset")
c.zadd("myzset", "a", 0, "b", 0, "c", 0, "d", 0, "e", 0)
c.zadd("myzset", "f", 0, "g", 0)
ret = c.execute_command("zlexcount", "myzset", "-", "+")
if str(ret) != "7":
    print "zlexcount" + " ret not compatible " + "redis: 7" + " checker: " + str(ret)
ret = c.execute_command("zlexcount", "myzset", "[b", "[f")
if str(ret) != "5":
    print "zlexcount" + " ret not compatible " + "redis: 5" + " checker: " + str(ret)

#zrangebylex
c.delete("myzset")
c.zadd("myzset", "a", 0, "aa", 0, "abc", 0, "apple", 0, "b", 0, "c", 0, "d", 0, "d1", 0, "dd", 0, "dobble", 0, "z", 0, "z1", 0)
ret = c.execute_command("zrangebylex", "myzset", "-", "+")
if ret != ['a', 'aa', 'abc', 'apple', 'b', 'c', 'd', 'd1', 'dd', 'dobble', 'z', 'z1']:
    print "zrangebylex" + " result wrong " + "redis: ['a', 'aa', 'abc', 'apple', 'b', 'c', 'd', 'd1', 'dd', 'dobble', 'z', 'z1']" + " checker: " + str(ret)
ret = c.execute_command("zrangebylex", "myzset", "-", "+", "limit", 0, 3)
if ret != ['a', 'aa', 'abc']:
    print "zrangebylex" + " result wrong " + "redis: ['a', 'aa', 'abc']" + " checker: " + str(ret)

#zrevrangebylex
c.delete("myzset")
c.zadd("myzset", "a", 0, "aa", 0, "abc", 0, "apple", 0, "b", 0, "c", 0, "d", 0, "d1", 0, "dd", 0, "dobble", 0, "z", 0, "z1", 0)
ret = c.execute_command("zrevrangebylex", "myzset", "+", "-")
if ret != ['z1', 'z', 'dobble', 'dd', 'd1', 'd', 'c', 'b', 'apple', 'abc', 'aa', 'a']:
    print "zrevrangebylex" + " result wrong " + "redis: ['z1', 'z', 'dobble', 'dd', 'd1', 'd', 'c', 'b', 'apple', 'abc', 'aa', 'a']" + " checker: " + str(ret)
ret = c.execute_command("zrevrangebylex", "myzset", "+", "-", "limit", 3, 3)
if ret != ['dd', 'd1', 'd']:
    print "zrangebylex" + " result wrong " + "redis: ['dd', 'd1', 'd']" + " checker: " + str(ret)


#zrangebyscore
c.delete("myzset")
c.zadd("myzset", "one", 1)
c.zadd("myzset", "two", 2)
c.zadd("myzset", "three", 3)
ret = c.execute_command("zrangebyscore","myzset", "-inf", "+inf")
if ret != ['one', 'two', 'three']:
    print "zrangebyscore" + " result wrong " + "redis: ['one', 'two', 'three']" + " checker: " + str(ret)

#zrank
c.delete("myzset")
c.zadd("myzset", "one", 1)
c.zadd("myzset", "two", 2)
c.zadd("myzset", "three", 3)
ret = c.execute_command("zrank", "myzset", "three")
if str(ret) != "2":
    print "zrank" + " result wrong " + "redis: 2" + " checker: " + str(ret)
ret = c.execute_command("zrank", "myzset", "four")
if ret != None:
    print "zrank" + " result wrong " + "redis: None" + " checker: " + str(ret)

#zrem
c.delete("myzset")
c.zadd("myzset", "one", 1)
c.zadd("myzset", "two", 2)
c.zadd("myzset", "three", 3)
ret = c.execute_command("zrem", "myzset", "two")
if str(ret) != "1":
    print "zrem" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.zrange("myzset", 0, -1, withscores=True)
if ret != [('one', 1.0), ('three', 3.0)]:
    print "zrem" + " result wrong " + "redis: [('one', 1.0), ('three', 3.0)]" + " checker: " + str(ret)

#zremrangebylex
c.delete("myzset")
c.zadd("myzset", "a", 0, "aa", 0, "abc", 0, "apple", 0, "b", 0, "c", 0, "d", 0, "d1", 0, "dd", 0, "dobble", 0, "z", 0, "z1", 0)
ret = c.execute_command("zremrangebylex", "myzset", "-", "+")
if str(ret) != "12":
    print "zremrangebylex" + " ret not compatible " + "redis: 7" + " checker: " + str(ret)
ret = c.zrangebylex("myzset", "-", "+")
if ret != []:
    print "zremrangebylex" + " result wrong " + "redis: []" + " checker: " + str(ret)

#zremrangebyrank
c.delete("myzset")
c.zadd("myzset", "one", 1, "two", 2, "three", 3)
ret = c.execute_command("zremrangebyrank", "myzset", 0, 1)
if str(ret) != "2":
    print "zremrangebyrank" + " ret not compatible " + "redis: 2" + " checker: " + str(ret)
ret = c.zrange("myzset", 0, -1, withscores=True)
if ret != [('three', 3.0)]:
    print "zremrangebyrank" + " result wrong " + "redis: [('three', 3.0)]" + " checker: " + str(ret)

#zremrangebyscore
c.delete("myzset")
c.zadd("myzset", "one", 1, "two", 2, "three", 3)
ret = c.execute_command("zremrangebyscore", "myzset", "-inf", "(2")
if str(ret) != "1":
    print "zremrangebyscore" + " ret not compatible " + "redis: 1" + " checker: " + str(ret)
ret = c.zrange("myzset", 0, -1, withscores=True)
if ret != [('two', 2.0), ('three', 3.0)]:
    print "zremrangebyscore" + " result wrong " + "redis: [('two', 2.0), ('three', 3.0)]" + " checker: " + str(ret)

#zrevrange
c.delete("myzset")
c.zadd("myzset", "one", 1, "two", 2, "three", 3)
ret = c.execute_command("zrevrange", "myzset", 0, -1)
if ret != ['three', 'two', 'one']:
    print "zrevrange" + " result wrong " + "redis: ['three', 'two', 'one']" + " checker: " + str(ret)

#zrevrangebyscore
c.delete("myzset")
c.zadd("myzset", "one", 1, "two", 2, "three", 3)
ret = c.execute_command("zrevrangebyscore", "myzset", "+inf", "-inf")
if ret != ['three', 'two', 'one']:
    print "zrevrangebyscore" + " result wrong " + "redis: ['three', 'two', 'one']" + " checker: " + str(ret)
ret = c.execute_command("zrevrangebyscore", "myzset", 2, 1)
if ret != ['two', 'one']:
    print "zrevrangebyscore" + " result wrong " + "redis: ['two', 'one']" + " checker: " + str(ret)
ret = c.execute_command("zrevrangebyscore", "myzset", "(2", "(1")
if ret != []:
    print "zrevrangebyscore" + " result wrong " + "redis: []" + " checker: " + str(ret)

#zrevrank
c.delete("myzset")
c.zadd("myzset", "one", 1, "two", 2, "three", 3)
ret = c.execute_command("zrevrank", "myzset", "one")
if str(ret) != "2":
    print "zrevrank" + " result wrong " + "redis: 2" + " checker: " + str(ret)
ret = c.execute_command("zrevrank", "myzset", "four")
if ret != None:
    print "zrevrank" + " result wrong " + "redis: None" + " checker: " + str(ret)

#zscore
c.delete("myzset")
c.zadd("myzset", "one", 1, "two", 2, "three", 3)
ret = c.execute_command("zscore", "myzset", "one")
if str(ret) != "1":
    print "zscore" + " result wrong " + "redis: 1" + " checker: " + str(ret)

#zunionstore
c.delete("zset1")
c.delete("zset2")
c.delete("out")
c.zadd("zset1", "one", 1)
c.zadd("zset1", "two", 2)
c.zadd("zset2", "one", 1)
c.zadd("zset2", "two", 2)
c.zadd("zset2", "three", 3)
ret = c.execute_command("zunionstore", "out", 2, "zset1", "zset2", "weights", 2, 3)
if str(ret) != "3":
    print "zunionstore" + " ret not compatible " + "redis: 3" + " checker: " + str(ret)
ret = c.zrange("out", 0, -1, withscores=True)
if ret != [('one', 5.0), ('three', 9.0), ('two', 10.0)]:
    print "zunionstore" + " result wrong " + "redis: [('one', 5.0), ('three', 9.0), ('two', 10.0)]" + " checker: " + str(ret)

print "check done"
