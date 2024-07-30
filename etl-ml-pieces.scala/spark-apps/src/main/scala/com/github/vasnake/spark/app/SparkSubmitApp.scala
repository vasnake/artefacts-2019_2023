/**
 * Created by vasnake@gmail.com on 2024-07-30
 */
package com.github.vasnake.spark.app

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import scala.util.Try

import com.beust.jcommander.JCommander

/**
 * Helps with commandline parameters, logger, spark session, spark config reader
 * @param appParams object with annotated fields, see jcommander API
 */
class SparkSubmitApp(appParams: Object) extends App with Serializable {
  @transient implicit val logger: Logger = LogManager.getLogger(this.getClass.getSimpleName)

  logger.info(s"App args: `${args.toSeq}`")
  new JCommander(appParams, null.asInstanceOf[java.util.ResourceBundle])
    .parse(args: _*)
  logger.info(s"App args parsed: `${appParams}`") // params written to appParams object

  @transient implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate() // session exists already, thanks to spark-submit commandline wrapper

  def getSparkConfParameterValue(key: String, default: String): String = Try(
    spark.conf.get(key, default) match {
      case "" => default
      case x @ _ => x
    }
  ).getOrElse(default)

  def getSparkConfParameterValue(key: String, default: Int): Int = Try(
    spark.conf.get(key, default.toString).toInt
  ).getOrElse(default)

}
