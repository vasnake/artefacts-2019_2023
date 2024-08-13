/** Created by vasnake@gmail.com on 2024-07-19
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import scala.reflect.ClassTag
import scala.util._

import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction

// TODO: add proper docstring
object functions {

  // Functions registry interface
  // Possible usage examples:
  // in scala code: `functions.registerAs("generic_avg", "gavg", spark, overrideIfExists = true)`
  // in python code: `spark._jvm.org.apache.spark.sql.catalyst.vasnake.udf.functions.registerAs("generic_avg", "gavg", spark._jsparkSession, True)`
  def registerAs(
    funcName: String,
    targetName: String,
    spark: SparkSession,
    overrideIfExists: Boolean = false,
  ): Unit = {
    val (info, builder, alias) = expressions(funcName) // possible NoSuchElementException

    spark
      .sessionState
      .catalog
      .registerFunction(
        CatalogFunction(
          FunctionIdentifier(if (targetName.isEmpty) alias else targetName),
          info.getClassName,
          Seq.empty,
        ),
        overrideIfExists = overrideIfExists,
        Some(builder),
      )
  }

  def registerAll(spark: SparkSession, overrideIfExists: Boolean = false): Unit =
    // Little late for builtins, it don't work here, see:
    // import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
    // FunctionRegistry.builtin.registerFunction(FunctionIdentifier(name), info, builder)

    expressions.foreach {
      case (name, (_, _, alias)) =>
        registerAs(name, alias, spark, overrideIfExists)
    }

  /** Provide access to `private[sql] def registerJava ...` function from `org.apache.spark.sql.UDFRegistration`.
    * @param funcName UDF name
    * @param classPath fully qualified class name that implements `org.apache.spark.sql.api.java`
    * @param spark session
    */
  def registerJava(
    funcName: String,
    classPath: String,
    spark: SparkSession,
  ): Unit =
    spark.udf.registerJava(funcName, classPath, null)

  // Functions collection

  // catalyst expressions collection: (name, alias)
  def names: Iterable[(String, String)] = expressions.map(rec => (rec._1, rec._2._3))

  // Custom Catalyst expressions, inspired by:
  // import org.apache.spark.sql.functions.stddev_pop
  // def stddev_pop(e: Column): Column = withAggregateFunction { StddevPop(e.expr) }
  // def stddev_pop(columnName: String): Column = stddev_pop(Column(columnName))
  // def map_concat(cols: Column*): Column = withExpr { MapConcat(cols.map(_.expr)) }
  // Also, look at:
  // import org.apache.spark.sql.catalyst.analysis.FunctionRegistry

  // Declarations for DataFrame API

  def generic_sum(columnName: String): Column = generic_sum(Column(columnName))
  def generic_sum(e: Column): Column = withAggregateFunction(GenericSum(e.expr))

  def generic_min(columnName: String): Column = generic_min(Column(columnName))
  def generic_min(e: Column): Column = withAggregateFunction(GenericMin(e.expr))

  def generic_max(columnName: String): Column = generic_max(Column(columnName))
  def generic_max(e: Column): Column = withAggregateFunction(GenericMax(e.expr))

  def generic_avg(columnName: String): Column = generic_avg(Column(columnName))
  def generic_avg(e: Column): Column = withAggregateFunction(GenericAvg(e.expr))

  def generic_most_freq(
    columnName: String,
    index: String = "null",
    threshold: String = "null",
    prefer: String = "null",
  ): Column = generic_most_freq(
    Column(columnName),
    sql.functions.expr(index).expr,
    sql.functions.expr(threshold).expr,
    sql.functions.expr(prefer).expr,
  )

  def generic_most_freq(
    e: Column,
    index: Expression,
    threshold: Expression,
    prefer: Expression,
  ): Column = withAggregateFunction {
    GenericMostFreq(e.expr, index, threshold, prefer)
  }

  def generic_coomul(a1: String, a2: String): Column = generic_coomul(Column(a1), Column(a2))
  def generic_coomul(col1: Column, col2: Column): Column = withExpr {
    GenericVectorCooMul(col1.expr, col2.expr)
  }

  def generic_semisum(a1: String, a2: String): Column = generic_semisum(Column(a1), Column(a2))
  def generic_semisum(col1: Column, col2: Column): Column = withExpr {
    GenericVectorSemiSum(col1.expr, col2.expr)
  }

  def generic_semidiff(a1: String, a2: String): Column = generic_semidiff(Column(a1), Column(a2))
  def generic_semidiff(col1: Column, col2: Column): Column = withExpr {
    GenericVectorSemiDiff(col1.expr, col2.expr)
  }

  def generic_matmul(a1: String, a2: String): Column = generic_matmul(Column(a1), Column(a2))
  def generic_matmul(col1: Column, col2: Column): Column = withExpr {
    GenericVectorMatMul(col1.expr, col2.expr)
  }

  def generic_isinf(columnName: String): Column = generic_isinf(Column(columnName))
  def generic_isinf(e: Column): Column = withExpr {
    GenericIsInf(e.expr)
  }

  def generic_isfinite(columnName: String): Column = generic_isfinite(Column(columnName))
  def generic_isfinite(e: Column): Column = withExpr {
    GenericIsFinite(e.expr)
  }

  // Declarations for SQL API

  type FunctionBuilder = Seq[Expression] => Expression

  private val expressions: Map[String, (ExpressionInfo, FunctionBuilder, String)] = Map(
    expression[GenericMin]("generic_min", "gmin"),
    expression[GenericMax]("generic_max", "gmax"),
    expression[GenericSum]("generic_sum", "gsum"),
    expression[GenericAvg]("generic_avg", "gavg"),
    expression[GenericMostFreq]("generic_most_freq", "most_freq"),
    expression[GenericVectorCooMul]("generic_coomul", "coomul"),
    expression[GenericVectorSemiSum]("generic_semisum", "semisum"),
    expression[GenericVectorSemiDiff]("generic_semidiff", "semidiff"),
    expression[GenericVectorMatMul]("generic_matmul", "matmul"),
    expression[GenericIsInf]("generic_isinf", "isinf"),
    expression[GenericIsFinite]("generic_isfinite", "isfinite"),
  )

  // Internals, see org.apache.spark.sql.catalyst.analysis.FunctionRegistry

  private def withExpr(expr: Expression): Column = Column(expr)

  private def withAggregateFunction(func: AggregateFunction, isDistinct: Boolean = false): Column =
    Column(
      func.toAggregateExpression(isDistinct)
    )

  private def expression[T <: Expression](
    name: String,
    alias: String,
  )(implicit
    tag: ClassTag[T]
  ): (String, (ExpressionInfo, FunctionBuilder, String)) = {
    // For `RuntimeReplaceable`, skip the constructor with most arguments, which is the main
    // constructor and contains non-parameter `child` and should not be used as function builder.
    val constructors = if (classOf[RuntimeReplaceable].isAssignableFrom(tag.runtimeClass)) {
      val all = tag.runtimeClass.getConstructors
      val maxNumArgs = all.map(_.getParameterCount).max
      all.filterNot(_.getParameterCount == maxNumArgs)
    }
    else
      tag.runtimeClass.getConstructors

    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = constructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))

    val builder = (expressions: Seq[Expression]) =>
      if (varargCtor.isDefined)
        // If there is an apply method that accepts Seq[Expression], use that one.
        Try(varargCtor.get.newInstance(expressions).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) =>
            // the exception is an invocation exception. To get a meaningful message, we need the cause.
            throw new AnalysisException(e.getCause.getMessage)
        }
      else {
        // Otherwise, find a constructor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])

        val ctor = constructors.find(_.getParameterTypes.toSeq == params).getOrElse {
          val validParametersCount = constructors
            .filter(_.getParameterTypes.forall(_ == classOf[Expression]))
            .map(_.getParameterCount)
            .distinct
            .sorted
          val expectedNumberOfParameters =
            if (validParametersCount.length == 1) validParametersCount.head.toString
            else
              validParametersCount
                .init
                .mkString("one of ", ", ", " and ") + validParametersCount.last

          throw new AnalysisException(
            s"Invalid number of arguments for function $name. " +
              s"Expected: $expectedNumberOfParameters; Found: ${params.length}"
          )
        }

        Try(ctor.newInstance(expressions: _*).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) =>
            // the exception is an invocation exception. To get a meaningful message, we need the cause.
            throw new AnalysisException(e.getCause.getMessage)
        }
      }

    (name, (expressionInfo[T](name), builder, alias))
  }

  private def expressionInfo[T <: Expression: ClassTag](name: String): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    val anno = clazz.getAnnotation(classOf[ExpressionDescription])
    if (anno != null)
      new ExpressionInfo(
        clazz.getCanonicalName,
        null, // db
        name,
        anno.usage(),
        anno.arguments(),
        anno.examples(),
        anno.note(),
        anno.since(),
      )
    else
      new ExpressionInfo(clazz.getCanonicalName, name)
  }
}
