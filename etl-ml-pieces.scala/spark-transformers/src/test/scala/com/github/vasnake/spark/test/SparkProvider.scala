/**
 * Created by vasnake@gmail.com on 2024-08-08
 */
package com.github.vasnake.spark.test

import org.scalatest._
import org.apache.spark.sql.SparkSession
import com.holdenkarau.spark.testing.SparkSessionProvider

// TODO: move to spark-io

trait SparkProvider {
  protected def loadSpark(): SparkSession
  @transient protected implicit lazy val spark: SparkSession = loadSpark()
}

trait LocalSpark extends SparkProvider with BeforeAndAfterAll { this: Suite =>

  def loadSpark(): SparkSession = SparkSessionProvider._sparkSession

  protected def sparkBuilder: SparkSession.Builder = SparkSession.builder()
    .master("local[2]")
    .appName(suiteName)
    .config("spark.driver.host", "localhost")
    .config("spark.ui.enabled", "false")
    .config("spark.app.id", suiteId)
    .config("spark.sql.shuffle.partitions", 4)
    .config("spark.checkpoint.dir", "/tmp/spark/checkpoints")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    SparkSessionProvider._sparkSession = sparkBuilder.getOrCreate()
  }

  override protected def afterAll(): Unit = {
    SparkSessionProvider._sparkSession.stop()
    SparkSessionProvider._sparkSession = null
    super.afterAll()
  }
}

trait SimpleLocalSpark extends LocalSpark { this: Suite =>

  override def sparkBuilder: SparkSession.Builder = super.sparkBuilder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", 1)
}

trait SimpleLocalSparkWithInfoLogger extends SimpleLocalSpark { this: Suite =>

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("INFO")
  }

  override protected def afterAll(): Unit = {
    spark.sparkContext.setLogLevel("WARN")
    super.afterAll()
  }
}
