pegasus-python-client
=====================

This is the official python client for [xiaomi/pegasus](https://github.com/XiaoMi/pegasus).

It uses [Twisted](http://twistedmatrix.com) for the asynchronous communication with pegasus server.

Installation
------------
Python 3.8+

pypegasus can be installed via pip as follows:

`git clone https://git.n.xiaomi.com/pegasus/pegasus-python-client`
`cd pegasus-python-client && python setup.py install`

Usage
-----
There are some basic guide in  [`sample.py`](sample.py).

Test
----
Before testing, you should firstly start an onebox cluster, referring to [`how to start an onebox cluster`](https://github.com/XiaoMi/pegasus/wiki/%E4%BD%93%E9%AA%8Conebox%E9%9B%86%E7%BE%A4).

## Basic interfaces test:

`cd tests && python -m unittest test_basics.TestBasics`

## Integration test:

`cd tests && python -m twisted.trial test_integration.py`

ATTENTION: you should firstly set proper pegasus shell path in [`test_integration.py`](test_integration.py#L10).

## Benchmark test:

`cd tests && python -m unittest test_benchmark.TestBasics`

The test result on my personal PC (CPU: Intel i7-7700 3.60GHz, mem: 8G) is:
```
10000 get cost: 3.80077195168 s, 0.000380077195168 s per op
10000 remove cost: 7.61887693405 s, 0.000761887693405 s per op
10000 set cost: 6.26366090775 s, 0.000626366090775 s per op
```
FAQ
---
### Q: When I run sample.py, error occured as following:
```
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
### A: pyOpenSSL version too low
the twisted version we used is 17.9.0, which requires pyOpenSSL>=16.0.0, you can

`pip install --upgrade pyopenssl`

ref: https://github.com/scrapy/scrapy/issues/2473
