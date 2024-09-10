# Bits and pieces for Spark3

Some modules from [etl-ml-pieces](../etl-ml-pieces.scala/readme.md), after migration to Spark3.

Spark 3.5.2; Scala 2.12.19; sbt 1.10.1; java 1.8; Hadoop 3.3.4; Hive 2.3.9

See https://github.com/apache/spark/blob/v3.5.2/dev/deps/spark-deps-hadoop-3-hive-2.3

uber-jar (`sbt assembly`): `target/scala-2.12/etl-ml-pieces-1923-assembly-1.0.0.jar`

```s
# Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties

# testQuick *JoinerAppTest* -- -z "make domain source from three sources with features selection"
# testQuick *JoinerAppTest* -- -z "build null map domain from null cols"

[error] Failed tests:
[error]         org.apache.spark.sql.catalyst.vasnake.udf.SemiSumTest
[error]         org.apache.spark.sql.catalyst.vasnake.udf.MapDecimalTest
[error]         org.apache.spark.sql.catalyst.vasnake.udf.SemiDiffTest
[error]         org.apache.spark.sql.catalyst.vasnake.udf.CooMulTest
[error]         org.apache.spark.sql.catalyst.vasnake.udf.MatMulTest

[error] Failed tests:          
[error]         com.github.vasnake.spark.io.hive.SQLHiveWriterTest

[info] - should create no-parts table *** FAILED ***
[info]   Success("CREATE TABLE IF NOT EXISTS `mydb`.`mytab` (a INT)  STORED AS ORC") did not equal Success("CREATE TABLE IF NOT EXISTS `mydb`.`mytab` (`a` INT)  STORED AS ORC") (SQLHiveWriterTest.scala:17)

[info] - should create partitioned table *** FAILED ***
[info]   Success("CREATE TABLE IF NOT EXISTS `mydb`.`mytab` (a INT) PARTITIONED BY (b STRING) STORED AS ORC") did not equal Success("CREATE TABLE IF NOT EXISTS `mydb`.`mytab` (`a` INT) PARTITIONED BY (`b` STRING) STORED AS ORC") (SQLHiveWriterTest.scala:25)

[info] - should create reference table *** FAILED ***
[info]   Success("CREATE TABLE IF NOT EXISTS `my_db`.`my_tab` (a INT, b FLOAT, c DOUBLE, d ARRAY<DOUBLE>, e MAP<BIGINT, SMALLINT>) PARTITIONED BY (dt STRING, uid_type STRING) STORED AS ORC") did not equal Success("CREATE TABLE IF NOT EXISTS `my_db`.`my_tab` (`a` INT, `b` FLOAT, `c` DOUBLE, `d` ARRAY<DOUBLE>, `e` MAP<BIGINT, SMALLINT>) PARTITIONED BY (`dt` STRING, `uid_type` STRING) STORED AS ORC") (SQLHiveWriterTest.scala:33)             

```
failed tests
