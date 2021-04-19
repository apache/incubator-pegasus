# pegic, another PEGasus Interactive Cli

Pegic is a user-facing command-line tool to read/write your data on Pegasus.

## Quick Start

Choose and download a suitable [release](https://github.com/pegasus-kv/pegic/releases) for your platform.

To connect to an existing Pegasus cluster, you need to specify the Pegasus address:

```sh
./pegic --meta 127.0.0.1:34601,127.0.0.2:34601
```

`127.0.0.1:34601,127.0.0.2:34601` is a list of MetaServer addresses. It's a comma (',') separated string.

Before starting read/write command, you need to firstly select a table to operate.

```
pegic » use xiaomi_ad_data
```

This command opens a table called "xiaomi_ad_data". Every operation followed is applied to this table.

## Commands

* [Set/Get/Del](#setgetdel)
* [Encoding](#encoding)
* [Scan](#scan)

This is an overview of the commands that pegic provides.

```txt
Commands:
  clear            clear the screen
  compression      read the current compression algorithm
  del              delete a record
  encoding         read the current encoding
  exit             exit the shell
  get              read a record from Pegasus
  help             use 'help [command]' for command help
  partition-index  ADVANCED: calculate the partition index where the hashkey is routed to
  scan             scan records under the hashkey
  set              write a record into Pegasus
  use              select a table
```

### Set/Get/Del

```
pegic » set "hashkey::1" "sortkey" "value::1"

ok

Encoding:
  - HashKey: UTF8
  - SortKey: UTF8
  - Value: UTF8
```

Set/Get/Del correspond to "Write","Read" and "Delete" to a table. Each manipulates on only single record.

```
pegic » get "hashkey::1" "sortkey"

hashkey::1 : sortkey : value::1

Encoding:
  - HashKey: UTF8
  - SortKey: UTF8
  - Value: UTF8
```

The first argument is the "hashkey", which is used for partitioning. The second argument is the "sortkey", that's key-ordering under the hashkey.

`set` command has the third argument, the "value", which is the record body, the main data.

### Encoding

Pegasus stores the entire record in raw bytes, the database is unaware of the actual encoding of the data. By default, when you use pegic, the "hashkey", "sortkey" and "value" will be encoded in UTF-8 from string to bytes.

```
pegic » set "中国" "北京" "abcdefg"

ok

Encoding:
  - HashKey: UTF8
  - SortKey: UTF8
  - Value: UTF8

pegic » get "中国" "北京"

中国 : 北京 : abcdefg

Encoding:
  - HashKey: UTF8
  - SortKey: UTF8
  - Value: UTF8  
```

However, the fields could also be set with other encodings like `ByteBuffer.allocate(4).putInt(1695609641).array()`, or be encoded via binary-serialization protocols like thrift/protobuf.

Here's an example where hashkey is encoded as an integer:

```
pegic » encoding hashkey int32

Encoding:
  - HashKey: INT32
  - SortKey: UTF8
  - Value: UTF8

pegic » get 19491001 "sortkey"

19491001 : sortkey : value

Encoding:
  - HashKey: INT32
  - SortKey: UTF8
  - Value: UTF8
```

If the encoding is not correctly matched to the data, the result will be unexpected.

```
pegic » encoding hashkey utf8

Encoding:
  - HashKey: UTF8
  - SortKey: UTF8
  - Value: UTF8

pegic » get 19491001 "sortkey"

Encoding:
  - HashKey: UTF8
  - SortKey: UTF8
  - Value: UTF8

error: record not found
HASH_KEY=19491001
SORT_KEY=sortkey
```

#### Supported encodings

- utf8

-	int32

-	int64

-	bytes: The golang byte array. Each byte is from range [0, 255]. For example: "104,101,108,108,111,32,119,111,114,108,100".

-	javabytes: The java byte array. Each byte is from range [-128, 127]. For example: "49,48,52,48,56,10,0,3,-1,-1,0".

-	asciihex: For example "\x00\x00\x00e\x0C\x00\x01\x08\x00\x01\x00\x00\x00e\x08".

### Compression

In some use-cases, we need to compress our value in order to improve read/write
performance and latency stability.

For those data, you need to specify the compression algorithm in pegic:

```
pegic » compression set zstd
ok

pegic » get novelcoldstart novelcoldstart
novelcoldstart : novelcoldstart : abcdef

TTL(Time-To-Live) is not set

Encoding:
  - HashKey: UTF8
  - SortKey: UTF8
  - Value: UTF8
Compression:
  zstd
```

### Scan

```
pegic » scan "中国"
中国 : 上海 : {"people_num": 35000000}
中国 : 北京 : {"people_num": 40000000}
中国 : 广州 : {"people_num": 28000000}
中国 : 成都 : {"people_num": 20000000}

Total records count: 4

Encoding:
  - HashKey: UTF8
  - SortKey: UTF8
  - Value: UTF8
```

`scan` iterates over all the records under the given hashkey. If you only want a subset of
the records within a given range, or to filter out the records that matched some pattern,
you can specify the flags according to your needs.

Scan with range:

```
pegic » scan --from 20200714000000 --to 20200715000000 uid18456112
uid18456112 : 20200714151429 : {"login": "Beijing"}
uid18456112 : 20200714152021 : {"login": "Beijing"}
uid18456112 : 20200714153342 : {"login": "Beijing"}

Total records count: 3

Encoding:
  - HashKey: UTF8
  - SortKey: INT64
  - Value: UTF8
```

Scan with filtering pattern:

```
pegic » scan --prefix rocksdb pegasus
pegasus : rocksdb_index_size_mb : 4300
pegasus : rocksdb_memtable_size_mb : 2000
pegasus : rocksdb_sst_size_mb : 358000

Total records count: 3

Encoding:
  - HashKey: UTF8
  - SortKey: UTF8
  - Value: INT64
```
