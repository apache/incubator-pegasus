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

# User Specified Compaction

## Summary

In Pegasus, sometimes we should add user specified compaction policy to reduce disk usage. This RFC proposes a user specified compaction design.

## Design

Add two classes named with `compaction_operation` and `compaction_rule`.

`compaction_filter_rule` represents the compaction rule to filter the keys which are stored in rocksdb.
There are three types of compaction operation:
- Hashkey rule, which supports prefix match, postfix match and anywhere match.
- Sortkey rule. Just like hashkey rule, it also supports prefix match, postfix match and anywhere match.
- TTL rule. It supports time range match with format [begin_ttl, end_ttl]

`compaction_operation` represents the compaction operation. A compaction operation will be executed when all the corresponding compaction rules are matched.
There are two types of compaction filter rule:
- Delete. It represents that we should delete this key when all the rules are matched.
- Update TTL. It represents that we should update TTL when all the rules are matched.

Finally, we should save the information about user specified compaction in app env. In order to make these information can still be retrieved after the machine is restarted.

## Class Diagram

Here is the class diagram for user specified compaction.

```
          +---------------+                              +-----------------+
          | compaction op +------------------------------+ compaction rule |
          +------^--------+                              +--------^--------+
                 |                                                |
        _________|_________                 ______________________|______________________
       |                   |               |                      |                      |
       |                   |               |                      |                      |
       |                   |               |                      |                      |
+------------+      +------------+  +--------------+      +--------------+       +--------------+
| update ttl |      |   delete   |  | hashkey rule |      | sortkey rule |       |    ttl rule  |
+------------+      +------------+  +--------------+      +--------------+       +--------------+
```
