/** Created by vasnake@gmail.com on 2024-07-30
  */
package com.github.vasnake.spark.app.datasets.joiner

import com.github.vasnake.core.text.StringToolbox
import com.github.vasnake.spark.app.datasets.joiner.config.UidImitationConfig
import org.apache.spark.sql

/** Not a universal solution.
  * A set of predefined constants used for declarations of a set of special columns in datasets.
  */
object implicits {
  val DT_COL_NAME: String = "dt"
  val UID_COL_NAME: String = "uid"
  val UID_TYPE_COL_NAME: String = "uid_type"

  val keyColumns: Seq[String] = Seq(UID_COL_NAME, DT_COL_NAME, UID_TYPE_COL_NAME)

  import sql.DataFrame
  import sql.functions.{ col, lit, expr }
  import sql.types.{ LongType, DataType }

  /** DataFrame extensions
    * @param ds dataset with certain restrictions applied to schema
    */
  implicit class RichDataset(private val ds: DataFrame) extends AnyVal {
    def setColumnsInOrder(withDT: Boolean, withUT: Boolean): DataFrame = {
      val dataCols = ds.columns.filter(n => !keyColumns.contains(n)).toSeq

      ds.select(
        UID_COL_NAME,
        dataCols ++
          (if (withDT) Seq(DT_COL_NAME) else Seq.empty) ++
          (if (withUT) Seq(UID_TYPE_COL_NAME) else Seq.empty): _*
      )
    }

    def filterDTPartition(dt: String): DataFrame = ds.where(col(DT_COL_NAME) === lit(dt))

    def filterUidTypePartitions(utypes: Option[List[String]]): DataFrame =
      utypes.map(lst => ds.where(col(UID_TYPE_COL_NAME).isin(lst: _*))).getOrElse(ds)

    def filterPartitions(partitions: List[Map[String, String]]): DataFrame = {
      import sql.Column

      val defaultFilter = col(DT_COL_NAME).isNotNull

      // col1_cond and col2_cond and ...
      def onePartitionFilter(row: Map[String, String]): Column = row.foldLeft(defaultFilter) {
        case (acc, (k, v)) => acc and (col(k) === lit(v))
      }

      val filters: List[Column] = partitions map onePartitionFilter

      // filter1 or filter2 or ...
      def combinedFilter: Column = filters.tail.foldLeft(filters.head) { // beware, empty filters will throw NoSuchElementException
        case (acc, filter) => acc or filter
      }

      ds.where(
        if (filters.nonEmpty) combinedFilter
        else defaultFilter
      )
    }

    def optionalWhere(filterExpr: Option[String]): DataFrame =
      filterExpr.map(expr => ds.where(expr)).getOrElse(ds)

    def imitateUID(fakeUid: Option[UidImitationConfig]): DataFrame = fakeUid
      .map(fu =>
        ds.drop(UID_TYPE_COL_NAME, UID_COL_NAME)
          .withColumnRenamed(fu.uid, UID_COL_NAME)
          .withColumn(UID_TYPE_COL_NAME, lit(fu.uid_type))
      )
      .getOrElse(ds)

    def dropInvalidUID: DataFrame = {
      // drop records where: uid_type in (OKID, VKID) and (uid is null or uid <= 0)
      val condition = col(UID_COL_NAME).isNull or {
        col(UID_TYPE_COL_NAME).isin("OKID", "VKID") and
          col(UID_COL_NAME).cast(LongType) <= 0L
      }

      ds.where(!condition)
    }

    def selectFeatures(featuresSelectExpressions: Option[List[String]]): DataFrame = {
      if (featuresSelectExpressions.getOrElse(List.empty[String]).nonEmpty)
        featuresSelectExpressions
      else None
    }.map(expressions =>
      ds.select(
        col(UID_COL_NAME) +:
          col(UID_TYPE_COL_NAME) +:
          expressions.map(e => expr(e)): _*
      ).dropRepeatedCols
    ).getOrElse(ds)

    def dropRepeatedCols: DataFrame = {
      import scala.collection.mutable

      val names = mutable.LinkedHashSet.empty[String]
      for (f <- ds.schema) names += f.name // preserve order

      ds.select(names.toList.map(col): _*)
    }

    def dropPartitioningCols(partitions: List[Map[String, String]], except: Set[String])
      : DataFrame = {
      val colnames = partitions
        .flatMap(p => p.keys)
        .toSet
        .toSeq
        .filter(cn => !except.contains(cn))

      ds.drop(colnames: _*)
    }

    def castColumnTo(colname: String, coltype: DataType): DataFrame = {
      val columns = ds
        .schema
        .map(f =>
          if (f.name.toLowerCase == colname.toLowerCase) col(f.name).cast(coltype) else col(f.name)
        )

      ds.select(columns: _*)
    }

    def selectCSVcols(csv: String, sep: String = ","): DataFrame = {
      import StringToolbox._
      import DefaultSeparators._

      ds.selectExpr(csv.splitTrim(sep): _*)
    }
  }
}
