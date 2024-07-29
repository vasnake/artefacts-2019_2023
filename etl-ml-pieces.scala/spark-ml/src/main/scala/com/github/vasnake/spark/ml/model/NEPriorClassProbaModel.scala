/**
 * Created by vasnake@gmail.com on 2024-07-29
 */
package com.github.vasnake.spark.ml.model

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.apache.spark.ml.linalg.{Vector => MLV} // ML Vector

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions => sqlf}

import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

//import com.github.vasnake.`ml-models`.{complex => models}
import com.github.vasnake.`ml-core`.models.NEPriorClassProba
import com.github.vasnake.spark.dataset.transform.GroupingColumnsServices
import com.github.vasnake.spark.io.{Logging => CustomLogging}
import com.github.vasnake.spark.ml.shared._
import com.github.vasnake.spark.dataset.Helpers.getNewTempColumnName

/**
  * NEPriorClassProba model (transformer), fitted in NEPriorClassProbaEstimator
  */
class NEPriorClassProbaModel(override val uid: String, val groupsConfig: List[(String, MLV)]) extends
  Model[NEPriorClassProbaModel] with
  MLWritable with
  NEPriorClassProbaParams with
  GroupingColumnsServices with
  CustomLogging with
  ParamsServices
{
  override def write: MLWriter = ???  // TODO: implement persistence according to spark.ml guidelines

  override def copy(extra: ParamMap): NEPriorClassProbaModel = {
    // can't use defaultCopy(extra) because you have config param in constructor
    val copied = new NEPriorClassProbaModel(uid, groupsConfig)
    copyValues(copied, extra).setParent(parent)
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // null and nan values will be transformed into null
    import NEPriorClassProbaModel.json
    logInfo(s"Transform dataframe, ${groupsConfig.length} aligners for groups: (${groupsConfig.map(_._1).mkString(", ")}), ...")
    logDebug(s"with params:\n${json(params.map(explain))};\naligners:\n${json(groupsConfig)}")

    validatePriorValues()
    require(getNumClasses > 1, s"Number of classes must be > 1, got `${getNumClasses}`")
    require(groupsConfig.map(_._2).forall(v => v.size == getNumClasses), "Aligner's size must be equal to `numClasses`")
    if (groupsConfig.isEmpty) logWarning("aligners list is empty, null values will be produced")

    // TODO: performance optimization, add parameter to control optional cast(inp as array<double>)
    val tempInputColName = getNewTempColumnName(dataset)
    val tempGroupsColName = "groups_" + tempInputColName
    val df = addTempGroupingColumn(dataset, getGroupColumns, tempGroupsColName)
      .withColumn(tempInputColName, sqlf.col(getInputCol).cast("array<double>"))

    val cfg: Broadcast[NEPriorClassProbaConfig] = df.sparkSession.sparkContext.broadcast(
      NEPriorClassProbaConfig(
        inputColIndex = df.schema.fieldIndex(tempInputColName),
        groupColIndex = df.schema.fieldIndex(tempGroupsColName),
        numClasses = getNumClasses,
        aligners = groupsConfig.map(t => (t._1, t._2.toArray)).toMap,
        outSchema = transformSchema(df.schema) // with validation
      )
    )

    // TODO: collect metrics, warnings and stats
    val res = NEPriorClassProbaModel.transform(df, cfg)
    logInfo("transform completed.")
    dropTempGroupingColumn(res, tempGroupsColName).drop(tempInputColName)
  }

}

object NEPriorClassProbaModel {

    def transform(df: DataFrame, config: Broadcast[NEPriorClassProbaConfig]): DataFrame = {
    // any invalid data on input => null in output
    df.mapPartitions(rows => {
      val cfg = config.value

      rows.map(row => {
        if (row.isNullAt(cfg.inputColIndex)) Row.fromSeq(row.toSeq :+ null) // invalid input should be reported
        else {
          val alignedProbs: Option[Array[Double]] =
            cfg.aligners.get(row.getString(cfg.groupColIndex)).flatMap(aligner => { // missing aligner should be reported
              decodeRow(row, cfg.inputColIndex, cfg.numClasses).map(probs => { // invalid input array should be reported
                align_probs_mutable(probs, aligner)
              })
            })

          Row.fromSeq(row.toSeq :+ alignedProbs.orNull)
        }
      })
    })(RowEncoder(config.value.outSchema))
  }

  // TODO: move vector checks to core model
  def decodeRow(row: Row, colIdx: Int, numClasses: Int): Option[Array[Double]] = {
    // given row is not null,
    // check for invalid input conditions: (size, null, nan, x < 0, sum == 0)
    val field = row.getAs[mutable.WrappedArray[Any]](colIdx)
    var sum: Double = 0

    if (field.length == numClasses) { // size OK
      val res: Array[Double] = new Array[Double](field.length) // n.b. null values become 0.0
      if (

        field.indices.exists(i => // invalid value exist?
          field(i) == null || {
            // not null ...
            val x = field(i).asInstanceOf[Double]
            if (x.isNaN || x < 0) true // invalid: isNaN or negative
            else { // valid: update sum, result
              res(i) = x
              sum += x
              false // value is valid
            }
          } // end of not null
        ) || sum == 0.0 // invalid row: sum == 0 or invalid value exists

      ) None // array items are null, nan, < 0 or sum == 0
      else Some(res) // array is ok
    } else None // wrong array size
  }

  @inline
  def align_probs_mutable(probs: Array[Double], aligner: Array[Double]): Array[Double] = {
    // TODO: consider using Vectors from ml.linalg
    // all checks performed already

    // mutation
    NEPriorClassProba(aligner).transform(probs)

    probs
  }

  import com.github.vasnake.jsonish.PrettyPrint.jsonify
  import com.github.vasnake.jsonish.PrettyPrint.implicits.jarray
  def json(x: Array[_]): String = jsonify(x)
  def json(x: List[(String, MLV)]): String = jsonify(x.map { case (name, vec) => s"group: $name; vec: ${vec.toString}" }.toArray)

}

case class NEPriorClassProbaConfig(
                                               inputColIndex: Int,
                                               groupColIndex: Int,
                                               numClasses: Int,
                                               aligners: Map[String, Array[Double]],
                                               outSchema: StructType
                                             )
