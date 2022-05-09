# pegasus-scala-client

This is the scala client for [xiaomi/pegasus](https://github.com/XiaoMi/pegasus).

It's built on top of the java client [xiaomi/pegasus-java-client](https://github.com/XiaoMi/pegasus-java-client).

## Features:

* Scala friendly.

* Serialize/deserialize automatically.

## Example:

```scala
    import com.xiaomi.infra.pegasus.scalaclient.Serializers._

    val c = ScalaPegasusClientFactory.createClient("resource:///pegasus.properties")
   
    // set/get/del
    val hashKey = 12345L
    c.set(table, hashKey, "sort_1", "value_1")
    val value = c.get(table, hashKey, "sort_1").as[String]
    c.del(table, hashKey, "sort_1")
    c.exists(table, hashKey, "sort_1") 
    c.sortKeyCount(table, hashKey)
    
    // multiset/multiget/multidel
    val values = Seq("sort_1" -> "value_1", "sort_2" -> "value_2", "sort_3" -> "value_3")
    val sortKeys = values.unzip._1 
    
    c.multiSet(table, hashKey, values)
    val count = c.sortKeyCount(table, hashKey)
    val multiGetValues = c.multiGet(table, hashKey, sortKeys).as[String]
    val multiGetSortKeys = c.multiGetSortKeys(table, hashKey)
    c.multiDel(table, hashKey, Seq("sort_1", "sort_2"))
    
    c.close
```