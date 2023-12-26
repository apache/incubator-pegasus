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
# pegasus-scala-client

It's built on top of the java client [apache/incubator-pegasus/java-client](https://github.com/apache/incubator-pegasus/tree/master/java-client).

## Features:

* Scala friendly.
* Serialize/deserialize automatically.

## Example:

```scala
    import org.apache.pegasus.scalaclient.Serializers._

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

## Development

### Format the code

Use scala format tool, see https://github.com/scalameta/scalafmt
```
sbt scalafmtSbt scalafmt test:scalafmt
```

### Run tests

NOTE: It requires the Pegasus [onebox](https://pegasus.apache.org/overview/onebox/) has been started.

Build Java dependency at first, then build and test Scala client.
```
cd ${PROJECT_ROOT}/java-client/scripts
./recompile_thrift.sh

cd ${PROJECT_ROOT}/java-client
mvn clean package -DskipTests -Dcheckstyle.skip=true
mvn clean install -DskipTests -Dcheckstyle.skip=true

cd ${PROJECT_ROOT}/scala-client
sbt test
```
