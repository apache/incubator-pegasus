# User Specified Compaction

## Summary

In Pegasus, sometimes we should add user specified compaction policy to reduce disk usage. This RFC proposes a user specified compaction design.

## Design

Add two classes named with `compaction_operation` and `compaction_rule`.

`compaction_filter_rule` represents the compaction rule to filter the keys which are stored in rocksdb.
There are three types of compaction filter rule:
1. Hashkey rule, which supports prefix match, postfix match and anywhere match.
2. Sortkey rule. Just like hashkey rule, it also supports prefix match, postfix match and anywhere match.
3. TTL rule. It supports time range match with format [begin_ttl, end_ttl]

`compaction_operation` represents the compaction operation. A compaction operation will be executed when all the corresponding compaction rules are matched.
There are two types of compaction filter rule:
1. Delete. It represents that we should delete this key when all the rules are matched.
2. Update TTL. It represents that we should update TTL when all the rules are matched.

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
