/** Created by vasnake@gmail.com on 2024-07-29
  */
package com.github.vasnake.spark.ml.model

import com.github.vasnake.`ml-core`.models._
import com.github.vasnake.spark.dataset.Helpers.getNewTempColumnName
import com.github.vasnake.spark.dataset.transform.GroupingColumnsServices
import com.github.vasnake.spark.io.{ Logging => CustomLogging }
import com.github.vasnake.spark.ml.shared._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType

class ScoreQuantileThresholdModel(
  override val uid: String,
  val groupsConfig: List[(String, ScoreQuantileThresholdConfig)],
) extends Model[ScoreQuantileThresholdModel]
       with MLWritable
       with ScoreQuantileThresholdParams
       with GroupingColumnsServices
       with CustomLogging
       with ParamsServices {
  override def write: MLWriter = ??? // TODO: implement persistence according to spark.ml examples

  override def copy(extra: ParamMap): ScoreQuantileThresholdModel = {
    // can't use defaultCopy(extra) because you have extra param in constructor
    val copied = new ScoreQuantileThresholdModel(uid, groupsConfig)
    copyValues(copied, extra).setParent(parent)
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // if group not found, null value will be produced;
    // null and nan values will be transformed to null.
    import ScoreQuantileThresholdModel.json
    logInfo(
      s"Transform dataframe, ${groupsConfig.length} models for groups: (${groupsConfig.map(_._1).mkString(", ")}), ..."
    )
    logDebug(s"with params:\n${json(params.map(explain))};\nmodels:\n${json(groupsConfig)}")

    // validation
    if (groupsConfig.isEmpty) logWarning("models list is empty, null values will be produced")
    validateClassesPriorLabels()

    val tempColumnWithGroupsNames = getNewTempColumnName(dataset)
    val df = addTempGroupingColumn(dataset, getGroupColumns, tempColumnWithGroupsNames)

    val cfg: Broadcast[ScoreQuantileThresholdModelConfig] = df
      .sparkSession
      .sparkContext
      .broadcast(
        ScoreQuantileThresholdModelConfig(
          models =
            groupsConfig.map { case (group, cfg) => (group, ScoreQuantileThreshold(cfg)) }.toMap,
          inputColIndex = df.schema.fieldIndex(getInputCol),
          groupColIndex = df.schema.fieldIndex(tempColumnWithGroupsNames),
          outSchema = transformSchema(df.schema), // with validation
          labelColSelected = isDefinedOutputCol,
          rankColSelected = isDefinedRankCol,
          labels = if (!isDefinedLabels) (1 to getNumClasses).map(_.toString).toArray else getLabels,
        )
      )

    // TODO: collect stats, metrics, warnings
    val res = ScoreQuantileThresholdModel.transform(df, cfg)
    logInfo("transform completed.")
    dropTempGroupingColumn(res, tempColumnWithGroupsNames)
  }
}

object ScoreQuantileThresholdModel {
  def transform(df: DataFrame, config: Broadcast[ScoreQuantileThresholdModelConfig]): DataFrame =
    // any invalid data on input => null in output
    df.mapPartitions { rows =>
      val cfg = config.value

      @inline def makeResultRow(
        row: Row,
        cls: => Any,
        rank: => Any,
      ): Row =
        if (cfg.labelColSelected && cfg.rankColSelected) Row.fromSeq(row.toSeq :+ cls :+ rank)
        else if (cfg.labelColSelected) Row.fromSeq(row.toSeq :+ cls)
        else Row.fromSeq(row.toSeq :+ rank)

      rows.map { row =>
        if (row.isNullAt(cfg.inputColIndex)) makeResultRow(row, null, null) // invalid input
        else
          cfg
            .models
            .get(row.getString(cfg.groupColIndex))
            .map { model =>
              val score = row.getDouble(cfg.inputColIndex)
              if (score.isNaN) makeResultRow(row, null, null) // invalid input should be reported
              else {
                val cls: Int = model.transform(score)
                def rank: Double = model.rank(score, cls)
                makeResultRow(row, cfg.labels(cls - 1), rank)
              }
            }
            .getOrElse(makeResultRow(row, null, null)) // missing model should be reported
      }
    }(RowEncoder(config.value.outSchema))

  // TODO: should be re-done
  import com.github.vasnake.jsonish.PrettyPrint.jsonify
  import com.github.vasnake.jsonish.PrettyPrint.implicits.jarray
  def json(x: Array[_]): String = jsonify(x)
  def json(x: List[(String, ScoreQuantileThresholdConfig)]): String =
    jsonify(x.map { case (name, cfg) => s"group: $name; cfg: ${cfg.toString}" }.toArray)
}

case class ScoreQuantileThresholdModelConfig(
  models: Map[String, ScoreQuantileThreshold],
  inputColIndex: Int,
  groupColIndex: Int,
  outSchema: StructType,
  labelColSelected: Boolean,
  rankColSelected: Boolean,
  labels: Array[String],
)
