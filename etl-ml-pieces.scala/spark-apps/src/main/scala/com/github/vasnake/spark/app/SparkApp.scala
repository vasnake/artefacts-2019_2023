package com.github.vasnake.spark.app

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession

/** Created by vasnake@gmail.com on 2024-08-07
  *
  * Mixin to add SparkSession env to your app, e.g.
  * {{{
  * object MySparkJob extends SparkApp {
  *  run { case args => implicit spark =>
  *    doStuffWithSparkAPI(args, spark) } }
  * }}}
  */
trait SparkApp extends App {
  type Job[R] = SparkSession => R

  /** Spark job main program
    *
    * @param handler your function, presumably in form of {{{
    *      run { case inputPath :: outputPath :: Nil => implicit spark => ??? }
    *      }}}
    * @tparam R return type of your program
    */
  def run[R](handler: PartialFunction[List[String], Job[R]]): Unit = {

    val job = handler.applyOrElse(
      args.toList,
      { wrong: List[String] =>
        throw new IllegalArgumentException(
          s"Wrong parameters, count: (${wrong.length});\n" + s"${wrong.map(s => s"`${s}`").mkString("\n")}"
        )
      }
    )

    // spark-submit conf, local master by default
    val conf = {
      val cnf = new SparkConf()
      if (cnf.get("master", "").isEmpty) cnf.setMaster("local") else cnf
    }

    implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    // There was some bug with custom hadoop properties in the yarn cluster mode; just use `hadoop.` prefix
    import scala.collection.JavaConverters._
    spark.sparkContext.hadoopConfiguration.asScala.foreach { entry =>
      SparkHadoopUtil.get.conf.set(entry.getKey, entry.getValue)
    }

    try job(spark)
    finally spark.stop()

  }
}
