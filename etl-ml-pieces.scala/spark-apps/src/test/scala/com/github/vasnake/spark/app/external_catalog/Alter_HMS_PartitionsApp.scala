package com.github.vasnake.spark.app.external_catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.expressions.Expression
import com.github.vasnake.spark.app.SparkApp
import com.github.vasnake.core.text.{StringToolbox => stb}

/*
Created by vasnake@gmail.com on 2024-08-07

Test: Update Hive table partitions params in metastore

{{{

```shell script

spark-submit --master yarn --deploy-mode cluster --name "metastore-tests" --verbose --queue root.dev.test.priority \
  --class com.github.vasnake.spark.app.external_catalog.Alter_HMS_PartitionsApp \
  --num-executors 1 --executor-cores 2 --executor-memory 2G --driver-memory 2G \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.speculation=false \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.dynamicAllocation.maxExecutors=2 --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.eventLog.enabled=true \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.hadoop.hive.exec.dynamic.partition=true \
  --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \
  --conf spark.hadoop.hive.exec.max.dynamic.partitions=100000 \
  --conf spark.hadoop.hive.exec.max.dynamic.partitions.pernode=10000 \
  --conf spark.kryoserializer.buffer.max=64m \
  hdfs:/user/${USER}/jars/etl-ml-pieces-latest.jar \
  foo=42;bar=baz qux=quux

```

}}}
 */
object Alter_HMS_PartitionsApp extends SparkApp {

  run { case args => implicit spark =>
    println(s"HiveMetastoreTestJob arguments: <${args.mkString(";")}>")
    runTests(spark, new Config(args))
  }

  def runTests(spark: SparkSession, config: Config): Unit = {
    println(s"runTests, config: `${config}`")

    val extCatalog = spark.sharedState.externalCatalog

    val db = "stage"
    val table = "test_audience"
    val predicates: Seq[Expression] = Seq.empty
    val tz = "UTC"

    val partitions: Seq[CatalogTablePartition] = extCatalog.listPartitionsByFilter(db, table, predicates, tz)
    if (partitions.isEmpty) sys.error("Empty list of partitions")

    val modifiedPartitions = for {
      partition <- partitions
    } yield partition.copy(parameters = partition.parameters.updated("markedAsDataLoaded", "true"))

    extCatalog.alterPartitions(db, table, modifiedPartitions)

    println("runTests done.")
  }

  class Config(args: Iterable[String]) {
    override def toString: String = cfg.toList.map(kv => s"${kv._1}=${kv._2}").sorted.mkString(";")

    // some int parameter
    def foo: Int = cfg.getOrElse("foo", "1").toInt

    // optional text parameter
    def bar: Option[String] = cfg.get("bar")

    private val cfg: Map[String, String] = {
      import stb.RichString
      // `;` parameters separator, if you have more than one in one arg
      // `=` key=value separator for each parameter
      implicit val separators: stb.Separators = stb.Separators(";", Some(stb.Separators("=")))

      for {
        line <- args
        kv <- line.parseMap
      } yield kv
    }.toMap

  }

}
