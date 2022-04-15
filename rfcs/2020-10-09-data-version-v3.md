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

# Pegasus Data Version v3

## Background

There are two RocksDB value-encoding formats in Pegasus. To speak simply, we call them v1 and v2. Here follows is the specification of the two formats:

```
v1: |- expire_ts (4bytes) -|- user value (bytes) -|
v2: |- expire_ts (4bytes) -|- timetag (8 bytes) -|- user value (bytes) -|
```

Currently, each table can have only one format. For example, a v2-formatted data can’t be written into a v1 table, otherwise it won’t be correctly translated.

Under the hood, Pegasus will retrieve the format version from table metadata during initialization. If it is v2, it writes and reads in v2. v1 likewise.

In the other words, the two formats v1 and v2 can not coexist in the same table (or the same RocksDB instance).

So there are a couple of problems in the current situation:
1. The BulkLoad process needs maintaining two implementations of RocksDB value generation. Multiple formats apparently increase the complexity of the external toolsets.
2. Extending one field in the value (v3, v4, v5...) will introduce one more format version, which subsequently leads to more maintenance work.
3. The old table is unable to upgrade to support the new format. Usually the intention we add a new field to RocksDB value is to introduce some useful features. For example, the timestag field since v2 allows the users to get the writing time of a record. However, the v1 table can never experience it, no matter what version Pegasus is.

Note that for the external toolkits that parse the raw RocksDB value, typically Pegasus Analyser, still it needs to maintain all the decoding methods of all data-format versions, including v1/v2/v3, even v4/v5 in the future. Because technically, we have no approach to guarantee that only one format of encoding exists in one table.

## Requirement

In general, our requirement is:

1. To provide a v3 version that supports downward compatibility. In another saying, it must support mixing of v1/v3 or v2/v3 data inside the table.
2. New data should be written only in v3, thus old-encoding records can gradually convert to new encoding. Data ingestion tools like BulkLoad write only in v3.
3. v3 must include a “version” field, which allows the table to be upgradable conveniently for future updates.

## Solution

```
v1: |- expire_ts (4bytes) -|- user value (bytes) -|
v2: |- expire_ts (4bytes) -|- timetag (8 bytes) -|- user value (bytes) -|
```

From the above encoding design, once the two versions mixed up within the table, it’s indistinguishable which version a record is.

`expire_ts` is the expiration timestamp of the user data. Suppose the TTL (Time-To-Live) is `ttl_secs`, and the current time is `unix_sec_now`.

```
expire_ts=ttl_secs+unix_sec_now - 2016/01/01 00:00:00（1451577600）
```

“- 2016/01/01 00:00:00” is a trick from the beginning of Pegasus that reduces the timestamp value to 4 bytes.

In that case, could we utilize the heading bits of `expire_ts` as the special mark of v3?

## Calculation

Suppose the heading x bits of `expire_ts` are acting as the v3 mark, which means, the maximum value of `expire_ts` could not exceed 2^(32-x).

If we use merely 1-bit mark for v3, it’s easily calculated that:

```
MAX(expire_ts)=2^31=2147483648
```

Since the formula of expire_ts is:

```
expire_ts=ttl_secs+unix_sec_now - 2016/01/01 00:00:00（1451577600）
```

Then `MAX(ttl_secs+unit_sec_now)=2147483648+1451577600=2084/1/19 03:14:08`

That is to say: unless the user TTL expires after 2084, we can always reserve one bit from the `expire_ts` to identify v3.

## v3

The encoding format of v3 is:

```
v3: |- 1bit -|- version (7bits) -|- expire_ts (4bytes) -|- timetag (8 bytes) -|- user value (bytes) -|
```

The starting 1-bit is the indicator whether it’s encoded in version after v2/v1. If the bit is 1, it describes the data-encoding version >=3.

The following 7 bits are the specific version number. v3 is “0000011”. The field can hold up to 128 different versions.

To avoid in case some users set the TTL to date beyond 2084 (possibly caused by bugs), we need a limit in both client and server that TTL should not exceed 10 years.

## Downward compatibility of v3

Pegasus stores the encoding version (called “PegasusDataVersion”) in a separate RocksDB column family (called “meta column”). Table reads and writes through the format.

After v3 released, “PegasusDataVersion” can keep unchanged. If the earlier version is v1, the value remains in v1.

If a record has the 1bit indicator to be 1, which describes it matches v>=3. We choose accordingly the decoding method from the 7-bit version.

If a record has the 1bit indicator to be 0, which indicates it is v1 or v2. We decode through “PegasusDataVersion”.

The following is the pseudocode:

```cpp
void init() {
  _old_ver = meta_column.get(PEGASUS_DATA_VERSION);
}
void read() {
  if(rocksdb_value[0] & (1<<7)) { // version >= 3
    version = rocksdb_value[0]&(^(1<<7)); // use the 7-bit version
  } else { // version == 1 || version == 2
    version = _old_ver; // use PegasusDataVersion
  }
  parse_value(rocksdb_value, version);
}

void write() {
  generate_value(..., v3);
}
```

## Upgrade from v3

It’s easy to upgrade the value version from v3. After v4 is released, we can use the 7-bit version for identification according to the above stated mechanism.

