/** Created by vasnake@gmail.com on 2024-07-19
  */
package com.github.vasnake.spark.udf

import scala.util._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.vasnake.udf.functions.{
  registerAs => _registerCatalyst,
  registerJava => _registerJava,
}
import org.apache.spark.sql.catalyst.vasnake.udf.{ functions => _catalystFuncs }

/** UDF and UDAF catalog.
  * Help clients to register functions before using them.
  * Usage examples:
  * {{{
  * com.github.vasnake.spark.udf.catalog.registerAll(spark, overrideIfExists = true)
  * com.github.vasnake.spark.udf.catalog.registerAs(name = "generic_avg", alias = "gavg", spark, overrideIfExists = true)
  * }}}
  * If catalog doesn't have a function for registration (UDF name not in the defined set, see below)
  * throw an exception of type `UdfRegistrationFailure`.
  *
  * Available functions:
  * {{{
  * generic_avg_hive
  * map_values_ordered
  * is_uint32
  * hash_to_uint32
  * murmurhash3_32_positive
  * uid64
  * map_join
  * uid2user
  * html_unescape
  * generic_min as gmin
  * generic_max as gmax
  * generic_sum as gsum
  * generic_avg as gavg
  * generic_most_freq as most_freq
  * generic_coomul as coomul
  * generic_semisum as semisum
  * generic_semidiff as semidiff
  * generic_matmul as matmul
  * generic_isinf as isinf
  * generic_isfinite as isfinite
  * }}}
  */
object catalog extends Logging {

  // main catalog interface, possible UdfRegistrationFailure exception
  def registerAs(
    name: String,
    alias: String,
    spark: SparkSession,
    overrideIfExists: Boolean = true,
  ): Unit = {
    logDebug(s"Registering UDF `${name}` as `$alias` override=$overrideIfExists")

    // 3 kind of udf, 1 reg func
    // Stage 1: search in sub-catalogs, result count could be: 0, 1, > 1; `1` is only correct answer
    // Stage 2: do register if count == 1

    def registerHive(classPath: String): Unit = {
      // According to https://spark.apache.org/docs/3.3.1/sql-ref-syntax-ddl-create-function.html
      val ifNotExists = if (overrideIfExists) "" else "IF NOT EXISTS"
      val expr = s"CREATE OR REPLACE TEMPORARY FUNCTION ${ifNotExists} ${alias} AS '${classPath}'"
      spark.sql(expr)
      logInfo(s"Registered Hive UDF: ${expr}")
    }

    def registerJava(classPath: String): Unit = {
      _registerJava(alias, classPath, spark)
      logInfo(s"Registered Java API UDF: `${classPath}` as `${alias}`")
    }

    def registerCatalyst(): Unit = {
      _registerCatalyst(name, alias, spark, overrideIfExists)
      logInfo(s"Registered Catalyst API UDF: `${name}` as `${alias}` override=$overrideIfExists")
    }

    def getRegisterProcedure: Try[() => Unit] = {
      // Search all possible functions with this name, map name to register procedure.
      // Error if found.count != 1
      val regProcs: List[() => Unit] = List(
        // cp: class path
        hiveFuncs.get(name).map(cp => () => registerHive(cp)),
        javaFuncs.get(name).map(cp => () => registerJava(cp)),
        catalystFuncs.get(name).map(_ => () => registerCatalyst()),
      ).flatten

      Try {
        require(regProcs.nonEmpty, "Can't find function in catalog")
        require(regProcs.length == 1, "Function name is not unique")

        regProcs.head
      }
    }

    // do registration
    val registerOrFail = getRegisterProcedure.map(_.apply())

    // check result
    registerOrFail match {
      case Success(_) => () // Happy path
      case Failure(err) =>
        logError(s"UDF registration failure, invalid function: `${name}` as `${alias}`", err)
        throw new UdfRegistrationFailure(s"`${name}` as `${alias}`", err)
    }

  }

  // helper
  def registerAll(spark: SparkSession, overrideIfExists: Boolean = true): Unit = {
    logDebug(s"Registering all UDFs in catalog, override=$overrideIfExists ...")

    catalystFuncs foreach {
      case (name, alias) =>
        registerAs(name, alias, spark, overrideIfExists)
    }

    (javaFuncs.keys ++ hiveFuncs.keys).foreach(name =>
      registerAs(name, name, spark, overrideIfExists)
    )

    logDebug(s"Registered all UDFs in catalog.")
  }

  // implementation details

  // name -> class path
  private val hiveFuncs: Map[String, String] = Map(
    "generic_avg_hive" -> "com.github.vasnake.hive.java.udaf.GenericAvgUDAF"
  )

  // name -> class path
  private val javaFuncs: Map[String, String] = Map(
    "map_values_ordered" -> "com.github.vasnake.spark.udf.`java-api`.MapValuesOrderedUDF",
    "is_uint32" -> "com.github.vasnake.spark.udf.`java-api`.CheckUINT32UDF",
    "hash_to_uint32" -> "com.github.vasnake.spark.udf.`java-api`.HashToUINT32UDF",
    "murmurhash3_32_positive" -> "com.github.vasnake.spark.udf.`java-api`.MurmurHash3_32UDF",
    "uid64" -> "com.github.vasnake.spark.udf.`java-api`.Uid64UDF",
    "map_join" -> "com.github.vasnake.spark.udf.`java-api`.MapJoinUDF",
    "uid2user" -> "com.github.vasnake.spark.udf.`java-api`.export.Uid2UserUDF",
    "html_unescape" -> "com.github.vasnake.spark.udf.`java-api`.HtmlUnescapeUDF",
  )

  // name -> alias
  private val catalystFuncs: Map[String, String] = _catalystFuncs.names.toMap

  class UdfRegistrationFailure(name: String) extends Exception(s"Can't register ${name} UDF") {
    def this(name: String, cause: Throwable) = {
      this(name)
      initCause(cause)
    }
  }
}
