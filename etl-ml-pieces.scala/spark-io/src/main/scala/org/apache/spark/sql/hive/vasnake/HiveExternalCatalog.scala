/**
 * Created by vasnake@gmail.com on 2024-07-23
 */
package org.apache.spark.sql.hive.vasnake

import com.github.vasnake.spark.io.{Logging => CustomLogging}
import com.github.vasnake.core.text.{StringToolbox => stb}

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.util.Utils

import org.apache.spark.sql.hive.{HiveExternalCatalog => HiveExternalCatalog_Orig}
import org.apache.spark.sql.hive.client.HiveClient

object HiveExternalCatalog extends CustomLogging {

  // API

  def openConnection(spark: SparkSession): ExternalCatalog = externalCatalog(spark.sparkContext)

  def closeConnection(conn: ExternalCatalog): Unit = {
    // Spark uses Hive and SessionState singleton in ThreadLocal wrapper, therefore correct pipeline looks like:
    // start thread -> open-or-use-already-opened thread connection -> do work -> close connection -> stop thread.
    // HiveMetaStoreClient.close called automagicaly on thread.close, thanks to ThreadLocal Hive.hiveDB

    // All I have to do: destroy instances of ExternalCatalog and associated SessionState

    // Real connection closed by
    // org.apache.hadoop.hive.metastore.HiveMetaStoreClient.close
    // extended by
    // org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient
    // called from
    // org.apache.hadoop.hive.ql.metadata.Hive.close
    // org.apache.hadoop.hive.ql.metadata.Hive.hiveDB // private static ThreadLocal[Hive] hiveDB
    // used in state.
    // That means, as soon as thread is closed, connection is closed automagically.
    // you don't need do shit to open/close connections.

    // Anyway, metastore take care of unused connections: https://github.com/apache/hive/blob/27dd6fdc89b10c8813c02310291b09565f0d4959/metastore/src/java/org/apache/hadoop/hive/metastore/HiveClientCache.java#L175
    // in short, every 2m it call cleanup task to remove garbage. At least it should.

    // unwrap HMC
    val ec: ExternalCatalog = conn.asInstanceOf[ExternalCatalogWithListener].unwrapped
    val hec = ec.asInstanceOf[HiveExternalCatalog_Orig] // BTW, locking source may be found here: private def withClient[T](body: => T): T = synchronized { ... }
    val hc: HiveClient = hec.client // HiveUtils.newClientForMetadata(conf, hadoopConf) // org.apache.spark.sql.hive.client.HiveClientImpl
    // via new IsolatedClientLoader(...).createClient() // org.apache.spark.sql.hive.client.IsolatedClientLoader

    // close state
    // x.y.z cannot be cast to x.y.z because of different class loaders,
    // reflection should work though
    val ss: Any = hc.getState // org.apache.hadoop.hive.ql.session.SessionState
    log.debug(s"Closing hive.ql.session SessionState: `${stb.repr(ss)}` ...")
    ss.getClass.getMethod("close").invoke(ss)
  }

  // implementation

  // Copied from org.apache.spark.sql.internal.SharedState.externalCatalog
  // reason: can't use `spark.sharedState.externalCatalog` in concurrent threads

  private def externalCatalog(sparkContext: SparkContext): ExternalCatalogWithListener = {

    val externalCatalog = reflect[ExternalCatalog, SparkConf, Configuration](
      externalCatalogClassName(sparkContext.conf),
      sparkContext.conf,
      sparkContext.hadoopConfiguration)

    // Wrap to provide catalog events
    val wrapped = new ExternalCatalogWithListener(externalCatalog)

    // Make sure we propagate external catalog events to the spark listener bus
    wrapped.addListener(new ExternalCatalogEventListener {
      override def onEvent(event: ExternalCatalogEvent): Unit = {
        sparkContext.listenerBus.post(event)
      }
    })

    wrapped
  }

  private val HIVE_EXTERNAL_CATALOG_CLASS_NAME = "org.apache.spark.sql.hive.HiveExternalCatalog"

  private def externalCatalogClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => HIVE_EXTERNAL_CATALOG_CLASS_NAME
      case "in-memory" => classOf[InMemoryCatalog].getCanonicalName
    }
  }

  /**
    * Helper method to create an instance of [[T]] using a single-arg constructor that
    * accepts an [[Arg1]] and an [[Arg2]].
    */
  private def reflect[T, Arg1 <: AnyRef, Arg2 <: AnyRef](
                                                          className: String,
                                                          ctorArg1: Arg1,
                                                          ctorArg2: Arg2)(
                                                          implicit ctorArgTag1: ClassTag[Arg1],
                                                          ctorArgTag2: ClassTag[Arg2]): T = {

    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag1.runtimeClass, ctorArgTag2.runtimeClass)
      val args = Array[AnyRef](ctorArg1, ctorArg2)
      ctor.newInstance(args: _*).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }

  }

}
