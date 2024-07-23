/**
 * Created by vasnake@gmail.com on 2024-07-23
 */
package com.github.vasnake.spark.io

import org.apache.spark.sql.DataFrame
import com.github.vasnake.spark.io.{Logging => Log}
import com.github.vasnake.common.file.FileToolbox

class IntervalCheckpointService(interval: Int, baseDir: String) extends CheckpointService with Log {

  private var currentStep: Int = 0
  logInfo(s"Created checkpoint service, interval: ${interval}, checkpoint basedir: `${baseDir}`")

  override def checkpoint(df: DataFrame): DataFrame = {
    currentStep += 1
    logDebug(s"Checkpoint step: ${currentStep}")

    if (currentStep % interval == 0) {
      logDebug(s"Step ${currentStep} hit interval ${interval} end, perform checkpoint ...")
      doCheckpoint(df)
    }
    else {
      logDebug(s"Step ${currentStep} inside interval ${interval}, skip checkpoint")
      df
    }
  }

  private def doCheckpoint(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    val checkpointDir = FileToolbox.joinPath(baseDir, s"checkpoint_step_${currentStep}")

    logInfo(s"Write df ${df.schema.simpleString} to parquet into `${checkpointDir}` ...")

    df
      .write
      .mode("overwrite")
      .option("compression", "gzip")
      .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
      .parquet(checkpointDir)

    logInfo(s"Read from parquet from `${checkpointDir}` ...")

    spark.read.parquet(checkpointDir)
  }

}
