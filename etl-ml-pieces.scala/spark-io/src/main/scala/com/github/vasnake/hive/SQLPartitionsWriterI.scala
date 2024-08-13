/** Created by vasnake@gmail.com on 2024-07-23
  */
package com.github.vasnake.hive

import scala.util.Try

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait SQLPartitionsWriterI {

  /** Build Hive `CREATE TABLE ...` DDL statement, e.g.
    * {{{
    * CREATE TABLE IF NOT EXISTS `mydb`.`mytable`(
    *  `uid` STRING,
    *  `score` FLOAT)
    * PARTITIONED BY (
    *  `dt` STRING,
    *  `uid_type` STRING)
    * STORED AS ORC
    * }}}
    * @param dbName name for a database in which table should be created, mandatory.
    * @param tableName name for table to create in given database, mandatory.
    * @param partitionColNames list of column names to be set as PARTITIONED BY columns, could be empty list for un-partitioned table.
    *                          If list is non-empty, names order in that list define partitions order in resulting DDL statement.
    * @param schema table schema as list of columns with names and data-types. Schema must contain all given partition columns
    *               and at least one non-partition column.
    *               Non-partition columns order in schema define columns order in resulting DDL statement.
    * @return `CREATE TABLE ...` DDL statement or error.
    */
  def createTableDDL(
    dbName: String,
    tableName: String,
    partitionColNames: Seq[String],
    schema: StructType,
  ): Try[String]

  /** Execute given SQL expression using `hiveql` dialect.
    * @param ddl SQL expression.
    * @return error or nothing.
    */
  def execHiveDDL(ddl: String): Try[Unit]

  /** Supposedly it:
    * 1) Create-or-replace temp. view from given dataframe,
    * 2) build Hive DML expression for insert one static partition into partitioned Hive table from created view, e.g.
    * {{{
    * INSERT OVERWRITE TABLE `mydb`.`mytable`
    * PARTITION(`dt`='2022-03-03', `uid_type`='VKID')
    * SELECT * FROM `mydb_mytable_source_temp_view`
    * }}}
    * @param dbName database name as part of table identification.
    * @param tableName table name as part of table identification.
    * @param partition list of pairs (partition-col-name, partition-value) for given partition.
    *                  Each `PARTITIONED BY` table column must be presented in that list.
    *                  Only STRING type of part. columns are supported.
    * @param df dataframe that contains all columns defined in table, partition columns are ignored.
    * @return `INSERT ...` DML statement or error.
    */
  def insertStaticPartDML(
    dbName: String,
    tableName: String,
    partition: Map[String, String],
    df: DataFrame,
  ): Try[String]

  /** Execute given SQL expression using `hiveql` dialect.
    * @param dml SQL expression.
    * @return error or resulting data.
    */
  def execHiveDML(dml: String): Try[DataFrame]
}
