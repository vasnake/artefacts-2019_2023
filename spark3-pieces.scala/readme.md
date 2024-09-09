# Bits and pieces for Spark3

Some modules from [etl-ml-pieces](../etl-ml-pieces.scala/readme.md), after migration to Spark3.

Spark 3.5.2; Scala 2.12.19; sbt 1.10.1; java 1.8; Hadoop 3.3.4; Hive 2.3.9

See https://github.com/apache/spark/blob/v3.5.2/dev/deps/spark-deps-hadoop-3-hive-2.3

uber-jar (`sbt assembly`): `target/scala-2.12/etl-ml-pieces-1923-assembly-1.0.0.jar`

```s
# testQuick *JoinerAppTest* -- -z "make domain source from three sources with features selection"
# testQuick *JoinerAppTest* -- -z "build null map domain from null cols"

# Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties

[error] Failed tests:          
[error]         com.github.vasnake.spark.io.hive.SQLHiveWriterTest

[error] Failed tests:
[error]         org.apache.spark.sql.catalyst.vasnake.udf.SemiSumTest
[error]         org.apache.spark.sql.catalyst.vasnake.udf.MapDecimalTest
[error]         org.apache.spark.sql.catalyst.vasnake.udf.SemiDiffTest
[error]         org.apache.spark.sql.catalyst.vasnake.udf.CooMulTest
[error]         org.apache.spark.sql.catalyst.vasnake.udf.MatMulTest

[error] Failed tests:
[error]         com.github.vasnake.spark.ml.estimator.ScoreEqualizerTest
[error]         com.github.vasnake.spark.ml.estimator.NEPriorClassProbaTest
[error]         com.github.vasnake.spark.ml.estimator.ScoreQuantileThresholdTest

[error] Failed tests:
[error]         com.github.vasnake.spark.app.datasets.JoinerAppTest

[info] - should build map domain dropping nulls on merge *** FAILED ***
[info]   Array("[42,OKID,null]") did not contain the same elements as List("[42,OKID,Map(s42 -> 42.0, d42 -> 142.0, f42 -> 242.0)]") (JoinerAppTest.scala:605)

[info] - should build map domain w/o duplicated keys *** FAILED ***
[info]   Array("[42,OKID,{a -> 1.0, b -> 2.0, c -> 3.0, d -> 4.0}]") did not contain the same elements as List("[42,OKID,[a -> 1.0, b -> 2.0, c -> 3.0, d -> 4.0]]") (JoinerAppTest.scala:634)
```
failed tests
