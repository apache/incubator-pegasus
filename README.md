# pegic, another PEGasus Interactive Cli

Pegic is an user-facing command-line tool to read/write your data on Pegasus.

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

This command opens a table called "xiaomi_ad_data". Every operations followed are applied to this table.

## Commands

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

If the encoding is not correctly matched to the data, the result could possibly
be not existing:

```
pegic » encoding hashkey utf8

Encoding:
  - HashKey: UTF8
  - SortKey: UTF8
  - Value: UTF8

pegic » get 19491001 "sortkey"
error: record not found
HASH_KEY=19491001
SORT_KEY=sortkey
```
