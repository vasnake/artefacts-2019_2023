/** Created by vasnake@gmail.com on 2024-07-23
  */
package com.github.vasnake.spark.io

import org.apache.spark.internal.{ Logging => SparkLogging }
import org.slf4j.MDC

/** Access to process id in log.
  * Setup log4j.properties with line like: log4j.appender.console.layout.ConversionPattern=... %X{PID} ...
  */
trait Logging extends SparkLogging {
  @transient lazy val pid: String = java.lang.management.ManagementFactory.getRuntimeMXBean.getName
  MDC.put("PID", pid)
}
