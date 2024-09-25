# Bits and pieces for Spark3

Spark 3.5.2; Scala 2.12.19; sbt 1.10.1; java 1.8; Hadoop 3.3.4; Hive 2.3.9
See https://github.com/apache/spark/blob/v3.5.2/dev/deps/spark-deps-hadoop-3-hive-2.3 for list of dependencies.

Some modules from [etl-ml Scala stuff](../etl-ml-pieces.scala/readme.md), after migration to Spark3.

Code revised and fixed, some 'TODO's resolved. I don't want to back-port those fixes, so you can consider
[etl-ml Scala stuff](../etl-ml-pieces.scala/readme.md) deprecated.

uber-jar (after building via `sbt assembly`) can be found in dir: `target/scala-2.12/etl-ml-pieces-1923-assembly-1.0.0.jar`

## New modules

Modules existing only here

- `com.github.vasnake.spark.dataset.transform.SampleFast`: in project `spark-transformers`,
DataFrame or Dataset transformation, can be used as 'limit' or 'take' or 'sample'. Select first N records from first M partitions.
If N is less than number of rows containing in the first partition, only first partition will be processed.
Can be very fast, by comparison to ordinary sampling methods.
