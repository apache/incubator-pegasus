## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![Build Status](https://travis-ci.org/facebook/rocksdb.svg?branch=master)](https://travis-ci.org/facebook/rocksdb)

RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on LevelDB by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it specially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/master/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Design discussions are conducted in https://www.facebook.com/groups/rocksdb.dev/


INSTALL ON WINDOWS
==================
Suppose that:
- use visual studio 2015
- put rDSN and rocksdb in `D:\work`
- have installed rDSN
- have write access to `C:\Windows\Temp`

Run cmake to generate visual studio project files:
> cd D:\work\rocksdb
> mkdir builder
> cd builder
> D:\work\rDSN\ext\cmake-3.2.2\bin\cmake .. -G "Visual Studio 14 2015 Win64" -DBOOST_INCLUDEDIR="D:\work\rDSN\ext\boost_1_59_0" -DBOOST_LIBRARYDIR="D:\work\rDSN\ext\boost_1_59_0\lib64-msvc-14.0"

Open `D:\work\rocksdb\builder\rocksdb.sln` in visual studio 2015 and build it.
