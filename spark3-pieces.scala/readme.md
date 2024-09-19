# Bits and pieces for Spark3

Some modules from [etl-ml-pieces](../etl-ml-pieces.scala/readme.md), after migration to Spark3.

Spark 3.5.2; Scala 2.12.19; sbt 1.10.1; java 1.8; Hadoop 3.3.4; Hive 2.3.9

See https://github.com/apache/spark/blob/v3.5.2/dev/deps/spark-deps-hadoop-3-hive-2.3

uber-jar (`sbt assembly`): `target/scala-2.12/etl-ml-pieces-1923-assembly-1.0.0.jar`

## new modules

Modules existing only here.

### spark-transformers

`com.github.vasnake.spark.dataset.transform.SampleFast` -
DF transformation, can be used as 'limit' or 'take' or 'sample'. Select first N records from first M partitions.
If N is less than number of rows containing in the first partition, only first partition will be processed.
Can be very fast, by comparison with ordinary sampling methods.
