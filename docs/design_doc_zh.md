# pegic, another PEGasus Interactive Cli

pegic设计面向用户，只提供业务方需要的命令。管理员，运维人员不使用该工具。
除了interactive模式，pegic也支持cli模式，可直接在bash上通过一条命令完成操作。

注：
- 下列 <> 表示不可为空。[] 表示可为空。
- 数据返回结果暂使用 tabular 方式展示，如有需要后期也可以支持json和csv。
- 不提供incr和checkandset/checkandmutate等低频接口。有需要时再提供。
- 不支持multiget，multiset，multidel接口，命令行不应该支持多条原子操作，使用过于复杂。

## 项目收益
- 使用Go编写，易用且易部署，不需编译，用户只需要下载binary即可。
- C++ Shell相关代码后续可删除，简化内核代码，降低C++项目复杂度，提升易维护性。
- Go拥有更易用的第三方命令行工具库，可以优雅地实现复杂的命令行提示。

## 访问集群
- 使用工具时传入meta地址： `./pegic -meta 127.0.0.1:34601 --meta 127.0.0.2:34601`
- 也可以传入http地址：`./pegic -http-url https://pegasus-gateway.hadoop.srv/c3srv-ad`
- 实际小米内部使用可以包一层脚本 `./pegic-run.sh -n c3srv-ad`, 通过 pegasus-gateway 来获取meta地址。

## 选择表

**Interactive 模式：`USE <"tablename">`**

选择表，此操作会直接拉取表的信息。

## 列出所有表

**Interactive 模式：`LS`**

目前只列出表名和创建表的时间。有需要还可以列出表限流。
后续支持权限认证后，可以支持只显示用户有权访问的表。

## 读取数据

**Interactive 模式：`GET <"hashkey"> <"sortkey">`**

同时也会返回TTL。

## 读取多条数据

**Interactive 模式：**

```
SCAN [COUNT|DELETE] <HASHKEY "hashkey">
[
    SORTKEY
    [BETWEEN <"startSortKey"> AND <"stopSortKey">]
    [CONTAINS <"filter">]
    [PREFIX <"filter">]
    [SUFFIX <"filter">]
]
[NOVALUE]
```

例子：
- `SCAN COUNT HASHKEY "uid:128245" SORTKEY CONTAINS "browser"`

备注：
- between, contains, prefix, suffix 只可选择其一。

## 全表扫描

## 写数据

**Interactive 模式：`SET <"hashkey"> <"sortkey"> <"value"> [ttl_secs]`**

## 删除数据

**Interactive 模式：`DEL <"hashkey"> <"sortkey">`**

## 数据编码

**Interactive 模式：**

```
ENCODING RESET
ENCODING HASHKEY <UTF-8 | ASCII | INT | BYTE>
ENCODING SORTKEY <UTF-8 | ASCII | INT | BYTE>
ENCODING VALUE <UTF-8 | ASCII | INT | BYTE>
```

设置读和写时使用的编码方式。 如果什么都不填则返回当前模式。"ENCODING RESET" 则重设当前全部编码为UTF-8。

- INT使用大端序，以INT64存储。允许有符号。
- 读写时，如果输入值是非法INT，非法ASCII，非法UTF-8，非法BYTE，则报错。
- 默认编码使用 UTF-8。
- BYTE 方式如： "0 1 -102 8 0 1 0 0 0 1 8 0 2 11 0 2 67 78" 类似 Java 的 Byte，是有符号整数，中间以空格拆分。
- 有需要可以再支持 HEX。

## 数据压缩

**Interactive 模式：`COMPRESSION <ZSTD | NO>`**
