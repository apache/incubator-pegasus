pegasus python client
=====================

This is the official python client for [xiaomi/pegasus](https://github.com/XiaoMi/pegasus).

It uses [Twisted](http://twistedmatrix.com) for the asynchronous communication with pegasus server.

Installation
------------
install from source:

`$ sudo python setup.py install`

Usage
-----
there are some basic guide in ``sample.py``

Test
----
####basic interfaces test:

`python -m twisted.trial test_basics.py`

####integration test:

`python -m twisted.trial test_integration.py`

####benchmark test:
`python -m twisted.trial test_benchmark.py`

test result on my personal PC(`CPU: Intel i7-7700 3.60GHz, mem: 8G`) is:

10000 get cost: 3.80077195168 s, 0.000380077195168 s per op

10000 remove cost: 7.61887693405 s, 0.000761887693405 s per op

10000 set cost: 6.26366090775 s, 0.000626366090775 s per op
