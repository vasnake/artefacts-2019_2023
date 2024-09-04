/**
 * Created by vasnake@gmail.com on 2024-08-14
 */
package com.github.vasnake.spark.io.hive

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec._
import org.scalatest.matchers._
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import scala.util.Success

class SQLHiveWriterTest extends AnyFlatSpec with should.Matchers {

  private lazy  val writer = SQLWriterFactoryImpl.getWriter(new WriterConfig { def spark: Option[SparkSession] = None })

  it should "create no-parts table" in {
    assert(
      writer.createTableDDL("mydb", "mytab", partitionColNames = Seq(), schema("a", "int"))
        ==
        Success("CREATE TABLE IF NOT EXISTS `mydb`.`mytab` (`a` INT)  STORED AS ORC")
    )
  }

  it should "create partitioned table" in {
    assert(
      writer.createTableDDL("mydb", "mytab", partitionColNames = Seq("b"), schema("a", "int", "b", "string"))
        ==
        Success("CREATE TABLE IF NOT EXISTS `mydb`.`mytab` (`a` INT) PARTITIONED BY (`b` STRING) STORED AS ORC")
    )
  }

  it should "create reference table" in {
    assert(
      writer.createTableDDL("my_db", "my_tab", partitionColNames = Seq("dt", "uid_type"),
        schema("a", "int", "b", "float", "c", "double", "d", "array<double>", "e", "map<long,short>", "uid_type", "string", "dt", "string")
      ) == Success(
      """CREATE TABLE IF NOT EXISTS `my_db`.`my_tab` (
          |`a` INT, `b` FLOAT, `c` DOUBLE, `d` ARRAY<DOUBLE>, `e` MAP<BIGINT, SMALLINT>
          |) PARTITIONED BY (
          |`dt` STRING, `uid_type` STRING
          |) STORED AS ORC""".stripMargin.replace("\n", ""))
    )
  }

  it should "fail creating table without non-partition columns" in {
    assert(
      writer.createTableDDL("mydb", "mytab", partitionColNames = Seq("b"), schema("b", "string")).toString
        ==
        "Failure(java.lang.IllegalArgumentException: requirement failed: At least one non-partition column required)"
    )
  }

  it should "fail on empty db name" in {
    assert(
      writer.createTableDDL("", "mytab", partitionColNames = Seq("b"), schema("a", "int", "b", "string")).toString
        ==
        "Failure(java.lang.IllegalArgumentException: requirement failed: Database name must be defined, got ``)"
    )
  }

  it should "fail on empty table name" in {
    assert(
      writer.createTableDDL("mydb", "", partitionColNames = Seq("b"), schema("a", "int", "b", "string")).toString
        ==
        "Failure(java.lang.IllegalArgumentException: requirement failed: Table name must be defined, got ``)"
    )
  }

  it should "fail if part. col not in schema" in {
    val actual = writer.createTableDDL("mydb", "mytab", partitionColNames = Seq("bb"), schema("a", "int", "b", "string")).toString
    val expected = """bb does not exist. Available: a, b"""
    assert(actual contains expected)
  }

  private def schema(cols: String*): StructType = {
    val nameTypeList = cols.sliding(2, 2)
    val fields = nameTypeList.map(_.toList) map {
      case colname :: coltype :: Nil => StructField(colname, dataType(coltype))
      case _ => sys.error("Check parameters list, it's size must be even")
    }

    StructType(fields.toArray)
  }

  private def dataType(datatype: String): DataType = {
    DataType.fromDDL(datatype)
  }

}
