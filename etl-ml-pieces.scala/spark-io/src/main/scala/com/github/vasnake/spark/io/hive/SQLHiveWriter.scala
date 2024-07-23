/**
 * Created by vasnake@gmail.com on 2024-07-23
 */
package com.github.vasnake.spark.io.hive

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.catalyst.util.quoteIdentifier

import scala.util.Try

import com.github.vasnake.spark.io.Logging
import com.github.vasnake.hive.SQLPartitionsWriterI
import com.github.vasnake.core.text.StringToolbox

class SQLHiveWriter(spark: => SparkSession) extends SQLPartitionsWriterI with Logging {

  def createTableDDL(dbName: String, tableName: String, partitionColumnsNames: Seq[String], schema: StructType): Try[String] = Try {
    logInfo(s"createTableDDL, db: `${dbName}`, table: `$tableName`, parts: [${partitionColumnsNames.mkString(",")}], schema: ${schema.simpleString}")
    require(dbName.nonEmpty, s"Database name must be defined, got `${dbName}`")
    require(tableName.nonEmpty, s"Table name must be defined, got `${tableName}`")

    // e.g. (`uid` STRING, `score` FLOAT)
    val columns: String = {
      val cols: Seq[StructField] = schema.fields.filter(field =>
        !partitionColumnsNames.contains(field.name)
      )
      require(cols.nonEmpty, "At least one non-partition column required")
      logDebug(s"createTableDDL, columns: [${cols.mkString(",")}]")

      s"(${columnsDDL(cols)})"
    }

    // e.g. PARTITIONED BY (`dt` STRING, `uid_type` STRING)
    val partitions: String = if (partitionColumnsNames.isEmpty) "" else {
      // no need for `require`, if any of parts not in schema we got IllegalArgumentException
      val parts: Seq[StructField] = partitionColumnsNames.map(name => schema(name))
      logDebug(s"createTableDDL, partitions: [${parts.mkString(",")}]")

      s"PARTITIONED BY (${columnsDDL(parts)})" // n.b. there is no check for `string` column type
    }

    val fqn = s"${quoteIdentifier(dbName)}.${quoteIdentifier(tableName)}"

    s"CREATE TABLE IF NOT EXISTS $fqn $columns $partitions STORED AS ORC"
  }

  def insertStaticPartDML(dbName: String, tableName: String, partition: Map[String, String], df: DataFrame): Try[String] = Try {
    logDebug(s"insertStaticPartDML, db: `${dbName}`, table: `$tableName`, parts: [${partition.mkString(", ")}], df.schema: ${df.schema.simpleString}")

    val (hiveParts, hiveCols) = hivePartsNopartsColumns(dbName, tableName) // check if db.table exists
    logDebug(s"Hive $dbName.$tableName part. cols: [${hiveParts.mkString(",")}];\nnon-part cols: [${hiveCols.mkString(",")}]")

    (hiveParts.map(_.name), hiveCols.map(_.name))
  } flatMap {
    case (hiveParts, hiveCols) =>
      insertStaticPartDML(dbName, tableName, partition, df, hiveParts, hiveCols)
  }

  def insertStaticPartDML(
                           dbName: String, tableName: String, partition: Map[String, String], df: DataFrame,
                           hiveParts: Seq[String], hiveCols: Seq[String]
                         ): Try[String] = Try {

    logInfo(s"insertStaticPartDML, db: `${dbName}`, table: `$tableName`, partition: [${partition.mkString(", ")}]")
    logInfo(s"from DataFrame ${df.rdd.getNumPartitions} partitions; schema: ${df.schema.simpleString}")
    logInfo(s"into Hive table `$dbName.$tableName`; part. cols: [${hiveParts.mkString(",")}]; non-part cols: [${hiveCols.mkString(",")}]")

    require(hiveParts.nonEmpty, "Hive table must be PARTITIONED BY at least one column")
    require(hiveCols.nonEmpty, "Hive table must have at least one non-partition column")

    val tempViewName = quoteIdentifier(s"${dbName}__${tableName}__${slugify(partition)}__source_temp_view")

    // only non-part. columns
    df.selectExpr(hiveCols : _*)
      .createOrReplaceTempView(tempViewName)

    // n.b. only STRING types as partitioning columns supported
    val partitionExpr = hiveParts.map(colName =>
      s"${quoteIdentifier(colName)}='${partition(colName)}'" // possible NoSuchElementException
    ).mkString(", ") // e.g. "dt='2022-03-03', uid_type='VKID'"

    val fqn = s"${quoteIdentifier(dbName)}.${quoteIdentifier(tableName)}"

    s"INSERT OVERWRITE TABLE $fqn PARTITION($partitionExpr) SELECT * FROM $tempViewName"
  }

  def execHiveDDL(ddl: String): Try[Unit] =
    execHiveDML(ddl).map(_ => ())

  def execHiveDML(dml: String): Try[DataFrame] = Try {
    logInfo(s"Execute query: `${dml}`")
    spark.sql("SET spark.sql.dialect=hiveql")
    spark.sql(dml)
  }

  private def columnsDDL(fields: Seq[StructField]): String =
    fields.map(columnDDL).mkString(", ")

  private def columnDDL(field: StructField): String =
    field.toDDL

  private def hivePartsNopartsColumns(db: String, table: String) = {
    val hiveColumns = spark.catalog.listColumns(db, table).collect()
    hiveColumns.partition(_.isPartition)
  }

  private def slugify[A, B](elements: Map[A, B]): String = {
    // Map(dt -> 2022-04-05, uid_type -> HID) => "dt_2022_04_05__uid_type_hid"
    val slugs = elements map {
      case (partName, partValue) =>
        StringToolbox.slugify(s"${partName.toString}_${partValue.toString}")
    }

    slugs.mkString("__")
  }

}

object SQLWriterFactoryImpl extends SQLWriterFactory {
  def getWriter(config: => WriterConfig): SQLPartitionsWriterI = {
    new SQLHiveWriter(config.spark.getOrElse(sys.error("SparkSession required")))
  }
}
