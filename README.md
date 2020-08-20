# Pegasus-Spark

Pegasus-Spark is the [Spark](https://spark.apache.org/) connector to Pegasus. We've provided several toolkits for
manipulate your Pegasus data:
- pegasus-analyser: pegasus-analyser can read the pegasus snapshot data stored in the remote filesystem(HDFS etc.)
  - Offline analysis of your Pegasus snapshot, see example: [count data](https://github.com/pegasus-kv/pegasus-spark/blob/8c585a47e4b618924275c5c1404bdaef9c26f40a/pegasus-spark-analyser/src/main/scala/com/xiaomi/infra/pegasus/spark/analyser/examples/basic/CountData.scala)
  - Transform your Pegasus snapshot into Parquet files, see example: [convert parquet](https://github.com/pegasus-kv/pegasus-spark/tree/8c585a47e4b618924275c5c1404bdaef9c26f40a/pegasus-spark-analyser/src/main/scala/com/xiaomi/infra/pegasus/spark/analyser/examples/parquet).
  - Compare your data which stored in two different pegasus clusters, see detail: [duplication verify](https://github.com/pegasus-kv/pegasus-spark/tree/8c585a47e4b618924275c5c1404bdaef9c26f40a/pegasus-spark-analyser/src/main/scala/com/xiaomi/infra/pegasus/spark/analyser/recipes/verify).
- pegasus-bulkloader: pegasus-bulkloader can convert source data to pegasus data and load into pegasus cluster with the [pegasus server 2.1](https://github.com/apache/incubator-pegasus/tree/v2.1) support, see example: [load csv data](https://github.com/pegasus-kv/pegasus-spark/blob/8c585a47e4b618924275c5c1404bdaef9c26f40a/pegasus-spark-bulkloader/src/main/scala/com/xiaomi/infra/pegasus/spark/bulkloader/examples/CSVBulkLoader.scala)
