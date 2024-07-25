/**
 * Created by vasnake@gmail.com on 2024-07-25
 */
package com.github.vasnake.spark.dataset.transform

import com.github.vasnake.text.evaluator._
import com.github.vasnake.text.parser._
import com.github.vasnake.core.text.{StringToolbox => stb}
import com.github.vasnake.spark.io.CheckpointService

import org.apache.spark.sql.DataFrame
import org.apache.log4j.{LogManager, Logger}

object Joiner {
  private def JOIN_COLUMNS_NAMES: Seq[String] = Seq("uid", "uid_type")

  @transient lazy val logger: Logger = LogManager.getLogger(this.getClass.getSimpleName)

  def parseJoinRule(rule: String, defaultItem: String): JoinExpressionEvaluator[String] = {
    // join rule in form of: "topics full_outer (profiles left_outer groups)"
    // or "" or "all_other_features"

    import stb._
    rule.splitTrim(Separators(" ")).toSeq match {
      case Seq() => SingleItemJoin(defaultItem.trim)
      case Seq(item) => SingleItemJoin(item)
      case Seq(_, _) => throw new IllegalArgumentException(s"Invalid config: malformed join rule `${rule}`")
      case _  => JoinRule.parse(rule)
    }
  }

  def joinDatasets(
                    datasets: Map[String, DataFrame],
                    joinRule: JoinExpressionEvaluator[String],
                    checkpointService: Option[CheckpointService] = None
                 ): DataFrame = {

    logger.info(s"Join rule: ${joinRule}")
    datasets.foreach { case (name, df) => logger.info(s"Dataset `${name}`: ${df.schema.mkString(";")}") }

    val names: Seq[String] = JoinRule.enumerateItems(joinRule)
    require(names.forall(datasets.isDefinedAt), "Unknown dataset name in join rule")

    val checkPointFun: Option[DataFrame => DataFrame] =
      checkpointService.map(cpService =>
        df => cpService.checkpoint(df)
      )

    JoinRule.join(joinRule, datasets, JOIN_COLUMNS_NAMES, checkPointFun)
  }

  object JoinRule {
    type DF = DataFrame
    type JE = JoinExpressionEvaluator[String]

    def parse(rule: String): JE = _parse[String](rule)(identity)

    def enumerateItems(tree: JE): Seq[String] = _enumerateItems[String](tree)(identity)

    def join(
              tree: JE,
              catalog: String => DF,
              keys: Seq[String],
              checkpoint: Option[DF => DF]
            ): DF = {

      tree.eval[DF] { case (left, right, join) =>
        val joined = left.join(right, keys, join)
        checkpoint.map(_checkpoint => _checkpoint(joined)).getOrElse(joined)
      }(identity, catalog)
    }

    private def _parse[T](rule: String)(implicit conv: String => T): JoinExpressionEvaluator[T] = JoinExpressionParser(rule) match {
      case JoinExpressionParser.Node(name) => SingleItemJoin[T](conv(name))
      case tree: JoinExpressionParser.Tree => TreeJoin[T](tree)
    }

    private def _enumerateItems[T](tree: JoinExpressionEvaluator[T])(implicit conv: String => T): Seq[T] = {
      implicit val ev: T => Seq[T] = t => Seq(t)

      tree.eval[Seq[T]] {
        case (left, right, _) => left ++ right
      }
    }

  }

}
