pegasus-python-client
=====================

This is the official python client for [xiaomi/pegasus](https://github.com/XiaoMi/pegasus).

It uses [Twisted](http://twistedmatrix.com) for the asynchronous communication with pegasus server.

Installation
------------
Python 2.x

It uses *setuptools* to retrieve and build the package as well as all dependent modules. So you
must make sure that setuptools is available on your system. Or else you can install it as this:

`$ sudo apt-get install python-setuptools`

Then install pegasus python client:

`$ sudo python setup.py install`

Usage
-----
There are some basic guide in  [`sample.py`](sample.py).

Test
----
## Basic interfaces test:

`python -m twisted.trial test_basics.py`

## Integration test:

`python -m twisted.trial test_integration.py`

## Benchmark test:

`python -m twisted.trial test_benchmark.py`

The test result on my personal PC (CPU: Intel i7-7700 3.60GHz, mem: 8G) is:
```
10000 get cost: 3.80077195168 s, 0.000380077195168 s per op
10000 remove cost: 7.61887693405 s, 0.000761887693405 s per op
10000 set cost: 6.26366090775 s, 0.000626366090775 s per op
```
