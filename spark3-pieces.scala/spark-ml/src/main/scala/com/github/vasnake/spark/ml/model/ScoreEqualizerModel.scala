/** Created by vasnake@gmail.com on 2024-07-29
  */
package com.github.vasnake.spark.ml.model

import com.github.vasnake.`ml-models`.{ complex => models }
import com.github.vasnake.spark.dataset.transform.GroupingColumnsServices
import com.github.vasnake.spark.io.{ Logging => CustomLogging }
import com.github.vasnake.spark.ml.shared._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._

import org.apache.spark.sql
import sql._
import sql.types.StructType

/** Stratified equalizer model fitted by estimator.
  *
  * N.b.: data records in groups that not found in given config will be considered invalid.
  * Null or nan score values will be transformed to null.
  */
class ScoreEqualizerModel(
  override val uid: String,
  val groupsConfig: List[(String, models.ScoreEqualizerConfig)]
) extends Model[ScoreEqualizerModel]
       with MLWritable
       with ScoreEqualizerParams
       with GroupingColumnsServices
       with CustomLogging
       with ParamsServices {
  override def write: MLWriter = ??? // TODO: implement persistence, according to spark.ml examples

  override def copy(extra: ParamMap): ScoreEqualizerModel = {
    // can't use defaultCopy(extra) because we have extra param in constructor
    val copied = new ScoreEqualizerModel(uid, groupsConfig)
    copyValues(copied, extra).setParent(parent)
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // if group not found, null value will be produced;
    // null and nan values will be transformed to null.
    import ScoreEqualizerModel.json
    logInfo(
      s"Transform dataframe, ${groupsConfig.length} equalizers for groups: (${groupsConfig.map(_._1).mkString(", ")}), ..."
    )
    logDebug(s"with params:\n${json(params.map(explain))};\nequalizers:\n${json(groupsConfig)}")

    if (groupsConfig.isEmpty) logWarning("equalizers list is empty, null values will be produced")

    val tempColumnWithGroupsNames = TEMP_GROUP_COLUMN
    val df = addTempGroupingColumn(dataset, getGroupColumns, tempColumnWithGroupsNames)

    val cfg: Broadcast[ScoreEqualizerModelConfig] = df
      .sparkSession
      .sparkContext
      .broadcast(
        ScoreEqualizerModelConfig(
          equalizers =
            groupsConfig.map { case (group, cfg) => (group, models.ScoreEqualizer(cfg)) }.toMap,
          inputColIndex = df.schema.fieldIndex(getInputCol),
          groupColIndex = df.schema.fieldIndex(tempColumnWithGroupsNames),
          outSchema = transformSchema(df.schema)
        )
      )

    // TODO: collect metrics, errors, warnings, stats, etc. send to metrics service (or/and log)
    val res = ScoreEqualizerModel.transform(df, cfg)
    logInfo("transform completed.")
    dropTempGroupingColumn(res, tempColumnWithGroupsNames)
  }
}

object ScoreEqualizerModel {
  def transform(df: DataFrame, config: Broadcast[ScoreEqualizerModelConfig]): DataFrame =
    // any invalid data in input => null in output
    df.mapPartitions { rows =>
      val cfg = config.value

      rows.map { row =>
        if (row.isNullAt(cfg.inputColIndex)) Row.fromSeq(row.toSeq :+ null) // invalid input, TODO: update metrics
        else { // TODO: could be faster
          val score: Option[Any] =
            cfg.equalizers.get(row.getString(cfg.groupColIndex)).flatMap { eq =>
              val score_raw = row.getDouble(cfg.inputColIndex)
              if (score_raw.isNaN) None
              else Some(eq.transform(score_raw)) // invalid input should be reported
            } // missing equalizer should be reported

          Row.fromSeq(row.toSeq :+ score.orNull)
        }
      }
    }(sql.Encoders.row(config.value.outSchema))

  import com.github.vasnake.jsonish.PrettyPrint.jsonify
  import com.github.vasnake.jsonish.PrettyPrint.implicits.jarray
  def json(x: Array[_]): String = jsonify(x)
  def json(x: List[(String, models.ScoreEqualizerConfig)]): String =
    jsonify(x.map { case (name, cfg) => s"group: $name; cfg: ${cfg.toString}" }.toArray)
}

case class ScoreEqualizerModelConfig(
  equalizers: Map[String, models.ScoreEqualizer],
  inputColIndex: Int,
  groupColIndex: Int,
  outSchema: StructType
)
