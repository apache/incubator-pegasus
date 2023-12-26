<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# pegasus-python-client

It uses [Twisted](http://twistedmatrix.com) for the asynchronous communication with pegasus server.

## Installation

Python 3.8+

pypegasus can be installed via pip as follows:

```
git clone https://github.com/apache/incubator-pegasus
cd inclubator-pegasus/python-client
thrift --gen py -out . ../idl/rrdb.thrift
thrift --gen py -out . ../idl/dsn.layer2.thrift
python3 setup.py install
```

## Usage

There are some basic guide in  [`sample.py`](sample.py).

## Test

Note: Before testing, you should firstly start an [onebox](https://pegasus.apache.org/overview/onebox/).

### Run sample

```shell
python3 sample.py
```

### Basic interfaces test

```shell
cd tests
python3 -m unittest test_basics.TestBasics
```

### Integration test

> Note: You should firstly set proper Pegasus shell path in test_integration.py.
> ```python
> shell_path = '/your/pegasus-shell/dir'
> ```

```shell
cd tests
python3 -m twisted.trial test_integration.py
```

## Benchmark test

```shell
cd tests
python3 -m unittest test_benchmark.TestBasics
```

The test result on my personal PC (CPU: Intel i7-7700 3.60GHz, mem: 8G) is:
```
10000 get cost: 3.80077195168 s, 0.000380077195168 s per op
10000 remove cost: 7.61887693405 s, 0.000761887693405 s per op
10000 set cost: 6.26366090775 s, 0.000626366090775 s per op
```

## FAQ

### Q: Error occurred about `'module' object has no attribute 'OP_NO_TLSv1_1'`:

Error details:
```shell
Traceback (most recent call last):
  File "./sample.py", line 3, in <module>
    from pgclient import *
  File "/home/lai/dev/pegasus-python-client/pgclient.py", line 7, in <module>
    from twisted.internet import reactor
  ...
  File "/usr/local/lib/python2.7/dist-packages/twisted/internet/_sslverify.py", line 38, in <module>
    TLSVersion.TLSv1_1: SSL.OP_NO_TLSv1_1,
AttributeError: 'module' object has no attribute 'OP_NO_TLSv1_1'
```

#### A: pyOpenSSL version too low
The twisted version we used is 17.9.0, which requires pyOpenSSL>=16.0.0, you can

```shell
pip install --upgrade pyopenssl
```

ref: https://github.com/scrapy/scrapy/issues/2473
