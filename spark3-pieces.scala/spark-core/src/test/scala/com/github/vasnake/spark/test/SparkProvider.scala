/** Created by vasnake@gmail.com on 2024-08-08
  */
package com.github.vasnake.spark.test

import com.holdenkarau.spark.testing.SparkSessionProvider
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.slf4j.MDC

trait SparkProvider {
  protected def loadSpark(): SparkSession
  @transient implicit protected lazy val spark: SparkSession = loadSpark()
  @transient protected lazy val pid: String = java.lang.management.ManagementFactory.getRuntimeMXBean.getName
}

trait LocalSpark extends SparkProvider with BeforeAndAfterAll { this: Suite =>
  def loadSpark(): SparkSession = SparkSessionProvider._sparkSession

  protected def sparkBuilder: SparkSession.Builder = SparkSession
    .builder()
    .master("local[2]")
    .appName(suiteName)
    .config("spark.driver.host", "localhost")
    .config("spark.ui.enabled", "false")
    .config("spark.app.id", suiteId)
    .config("spark.sql.shuffle.partitions", 4)
    .config("spark.checkpoint.dir", "/tmp/spark/checkpoints")

  override protected def beforeAll(): Unit = {
    // appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} - %p - %c{1} - [pid %X{PID}] - %m%n%ex
    // appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} - %p - %c{1} - %m%n%ex - [pid/tid %processId/%threadId %threadName]
    MDC.put("PID", pid)

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
  override def sparkBuilder: SparkSession.Builder = super
    .sparkBuilder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", 1)
}

trait SimpleLocalSparkWithHive extends LocalSpark { this: Suite =>
  override def sparkBuilder: SparkSession.Builder = super
    .sparkBuilder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", 1)
    .enableHiveSupport()
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
