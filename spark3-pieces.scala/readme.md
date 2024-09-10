# Bits and pieces for Spark3

Some modules from [etl-ml-pieces](../etl-ml-pieces.scala/readme.md), after migration to Spark3.

Spark 3.5.2; Scala 2.12.19; sbt 1.10.1; java 1.8; Hadoop 3.3.4; Hive 2.3.9

See https://github.com/apache/spark/blob/v3.5.2/dev/deps/spark-deps-hadoop-3-hive-2.3

uber-jar (`sbt assembly`): `target/scala-2.12/etl-ml-pieces-1923-assembly-1.0.0.jar`

```s
# cd json
# assembly

java.lang.RuntimeException:
[error] Deduplicate found different file contents in the following:
[error]   Jar name = jackson-annotations-2.12.2.jar, jar org = com.fasterxml.jackson.core, entry target = module-info.class
[error]   Jar name = jackson-core-2.12.2.jar, jar org = com.fasterxml.jackson.core, entry target = module-info.class
[error]   Jar name = jackson-databind-2.12.2.jar, jar org = com.fasterxml.jackson.core, entry target = module-info.class

```
assembly errors
