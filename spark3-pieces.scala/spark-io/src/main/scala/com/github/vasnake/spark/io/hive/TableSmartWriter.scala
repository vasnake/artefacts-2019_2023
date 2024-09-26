/** Created by vasnake@gmail.com on 2024-07-23
  */
package com.github.vasnake.spark.io.hive

import com.github.vasnake.common.num.{ FastMath => fm }
import com.github.vasnake.core.text.StringToolbox.repr
import com.github.vasnake.spark.io.stats.DataFrameBucketsStats
import com.github.vasnake.spark.io.{ Logging => CustomLogging }

import scala.util._

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import org.apache.spark.sql.{ functions => sqlfn, _ }
import org.apache.spark.sql.catalog.{ Column => CatalogColumn }
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{ Expression => CatalystExpression }
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.hive.vasnake.MetastoreQueryProcessorWithConnPool
import org.apache.spark.sql.types.DataTypes

import org.slf4j.Logger

class TableSmartWriter(partitionMethodOrNull: String) extends CustomLogging {

  // default constructor
  def this() = this(TableSmartWriter.DEFAULT_PARTITIONER)

  import TableSmartWriter._ // companion object functions

  private val partitionMethod: String = Option(partitionMethodOrNull) match {
    case Some(x) => x // SQL, RDD
    case None => DEFAULT_PARTITIONER
  }

  def insertIntoHive(
    df: DataFrame,
    database: String,
    table: String,
    maxRowsPerBucket: Int,
    overwrite: Boolean,
    raiseOnMissingColumns: Boolean,
    checkParameterOrNull: String
  ): Unit = {

    logInfo(
      s"Inserting DataFrame `${df.schema.simpleString}`\ninto Hive table `${database}.${table}`\nwith parameters: " +
        s"rows-per-file: ${maxRowsPerBucket}; overwrite: $overwrite, raiseOnMissingColumns: $raiseOnMissingColumns, " +
        s"checkParameter: ${repr(checkParameterOrNull)}, partitionMethod: `$partitionMethod`"
    )

    debugShow(df, message = "JVM insertIntoHive, input DF:")

    implicit val spark: SparkSession = df.sparkSession
    implicit val logger: Logger = this.log
    val checkParameter = Option(checkParameterOrNull)

    require(database.nonEmpty, "Database name can't be empty")
    require(table.nonEmpty, "Table name can't be empty")
    require(maxRowsPerBucket > 0, "Rows/bucket must be `> 0`")
    require(
      checkParameter.forall(!RESERVED_PART_PARAM_NAMES.contains(_)),
      s"Partition parameter name can't be in reserved names set (${RESERVED_PART_PARAM_NAMES.mkString(", ")})"
    )

    val hiveColumns: Seq[CatalogColumn] = spark.catalog.listColumns(database, table).collect()
    val partColumns: Seq[CatalogColumn] = hiveColumns.filter(_.isPartition)
    val partColumnNames: Seq[String] = partColumns.map(_.name)

    logDebug(s"Hive table structure: ${hiveColumns.mkString(", ")}")

    // part. columns in catalog
    require(
      partColumns.forall(_.dataType == "string"),
      s"Hive table partition columns must be of type `string`, got ${partColumns.mkString(", ")}"
    )

    // part. columns in DF
    require(
      partColumnNames.forall(
        df.schema(_).dataType.simpleString == DataTypes.StringType.simpleString
      ), // ignore nullability
      s"Dataframe partition columns must by of type `string`, got ${df.schema.simpleString}"
    )

    if (partColumnNames.isEmpty)
      logWarning(s"Table `${database}.${table}` doesn't have partitioning columns")
    else logInfo(s"Partition columns: (${partColumnNames.mkString(", ")})")

    val result: Try[Unit] = for {
      fixedDF <- fixDataFrameStructure(
        df,
        hiveColumns.map(_.name),
        recoverMissingCols = !raiseOnMissingColumns
      )
      (partDF, partStats, bucketingErrorsCount) <- repartitionToBuckets(
        fixedDF,
        partColumnNames,
        maxRowsPerBucket,
        partitionMethod
      )
      // remove success flag if present
      _ <- alterPartitionsParameters(
        database,
        table,
        partStats,
        checkParameter.map(name => Map(name -> None)).getOrElse(Map.empty),
        failIfNotFoundPartitions = false
      )
      // actual insert
      _ <- write(partDF, database, table, overwrite)
      _ <-
        if (bucketingErrorsCount.value > 0)
          Failure(new RuntimeException(s"Bucketing errors count: ${bucketingErrorsCount.value}"))
        else Success(Unit)
      // set success flag if present
      _ <- alterPartitionsParameters(
        database,
        table,
        partStats,
        checkParameter.map(name => Map(name -> Some("true"))).getOrElse(Map.empty)
      )
    } yield ()

    // error logging and escalating
    result match {
      case Failure(exception) =>
        logError(
          s"Failed inserting dataframe into Hive table `${database}.${table}`: ${exception.getMessage}",
          exception
        )
        throw exception
      case _ => logInfo("Dataframe successfully inserted.")
    }
  }
}

object TableSmartWriter {
  def fixDataFrameStructure(
    df: DataFrame,
    hiveColumns: Seq[String],
    recoverMissingCols: Boolean
  )(implicit
    log: Logger
  ): Try[DataFrame] = Try {

    log.info(s"Fixing dataframe columns, recoverMissingCols: ${recoverMissingCols} ...")

    // no need for column type casting, spark writer will cast anyway
    val cols: Seq[Column] = hiveColumns.map { colname =>
      if (recoverMissingCols && !df.columns.contains(colname)) {
        log.warn(s"missing column `${colname}`, recovering with null")
        sqlfn.lit(null).alias(colname)
      }
      else sqlfn.col(colname)
    }

    val res = df.select(cols: _*)
    debugShow(res, "Fixed dataframe columns:")
    res
  }

  def repartitionToBuckets(
    df: DataFrame,
    partitionColumnsNames: Seq[String],
    maxRowsPerBucket: Int,
    partitionMethod: String
  )(implicit
    log: Logger
  ): Try[(DataFrame, Option[DataFrameBucketsStats], LongAccumulator)] = Try {

    // collecting partitions stats
    // repartition DF

    val errorsCountAccum = df.sparkSession.sparkContext.longAccumulator("BucketingErrors")

    if (partitionColumnsNames.isEmpty) {
      log.info("Repartition flat table ...")

      val rowsCount = df.count()
      if (rowsCount > 0) {
        val numParts = fm.ceil(rowsCount.toDouble / maxRowsPerBucket.toDouble).toInt
        log.info(
          s"Flat table repartition: rows: ${rowsCount}, rows-per-bucket: ${maxRowsPerBucket}, partitions: ${numParts}"
        )
        // result
        (df.repartition(numParts), None, errorsCountAccum)
      }
      else {
        log.warn("Flat table have no rows, dataframe is empty")
        // result
        (df, None, errorsCountAccum)
      }
    }
    else
      {
        log.info(s"Processing partitioned table, selected method: `${partitionMethod}` ...")

        for {
          partitionsStats <- _collectPartitionsStats(df, partitionColumnsNames, maxRowsPerBucket)

          bucketedDF <- partitionMethod match {
            case RDD_PARTITIONER =>
              _repartitionToBucketsRdd(df, partitionsStats, maxRowsPerBucket, errorsCountAccum)
            case _ =>
              _repartitionToBucketsSql(df, partitionsStats, maxRowsPerBucket, errorsCountAccum)
          }
        } yield (bucketedDF, partitionsStats, errorsCountAccum)
      }.get // produce value or exception from inner Try
  }

  def _collectPartitionsStats(
    df: DataFrame,
    partitionColumns: Seq[String],
    maxRowsPerBucket: Int
  )(implicit
    log: Logger
  ): Try[Option[DataFrameBucketsStats]] =
    Try {
      log.info("Collecting partitions statistics ...")

      val stats = DataFrameBucketsStats(partitionColumns, df, maxRowsPerBucket) // group-by, count, collect

      if (stats.dataFramePartitions.isEmpty) {
        log.warn("Collected no data, dataframe is empty")
        None
      }
      else {
        log.info(s"Collected partitions statistics: ${DataFrameBucketsStats.summaryMessage(stats)}")
        log.debug(
          s"partitions (first 20 from ${stats.dataFramePartitions.length}):\n`${stats.dataFramePartitionsStats.take(20).mkString(";\n")}`"
        )
        Some(stats)
      }
    }

  def _repartitionToBucketsRdd(
    df: DataFrame,
    partitionsInfo: Option[DataFrameBucketsStats],
    maxRowsPerBucket: Int,
    errorsCountAccum: LongAccumulator
  )(implicit
    log: Logger
  ): Try[DataFrame] = Try {

    val partitionedDF: DataFrame = partitionsInfo match {
      case None => // empty DF
        log.warn("Repartitioning rows to buckets (BucketPartitioner), dataframe is empty")
        df
      case Some(partsInfo) =>
        log.info("Repartitioning rows to buckets (BucketPartitioner) ...")
        // N.B. DF->RDD->DF transformation costs an extra stage with SerDes ops
        val sc = df.sparkSession.sparkContext

        log.debug("Creating a PairRDD ...")
        val bcPartsColsIndices: Broadcast[Seq[Int]] =
          sc.broadcast(partsInfo.partitionColumnsIndices)

        // df DAG ends here
        if (DEBUG_MODE) df.explain(extended = true)

        // partition id -> data row
        val kvRdd: RDD[(String, Row)] = df
          .rdd
          .keyBy(row =>
            bcPartsColsIndices
              .value
              .map { i =>
                if (row.isNullAt(i)) sys.error("NULL in partition columns is not allowed")
                else row.getString(i)
              }
              .mkString("/")
          )

        // N.B. Only stdlib types allowed in broadcast because of the `SQL ADD JAR ...` serialization problem, e.g:
        // java.lang.ClassCastException: com...DataFrameHivePartitionsInfo cannot be cast to com...DataFrameHivePartitionsInfo

        log.debug("Repartitioning the PairRDD ...")
        // broadcast parameters, tuple of:
        // 1: random
        // 2: num_partitions = parts.map(_.bucketsCount).sum
        // 3: buckets_map: (part_id -> (first_bucket, buckets_count))
        val bcParams: Broadcast[(Random, Int, Map[String, (Int, Int)])] = sc.broadcast(
          (
            new Random(),
            partsInfo.dataFramePartitionsStats.values.map(_.bucketsCount).sum,
            partsInfo.dataFramePartitionsStats.map {
              case (k, v) => k -> (v.firstBucket, v.bucketsCount)
            }
          )
        )

        val bucketedKvRdd = kvRdd.partitionBy(new BucketPartitioner(bcParams, errorsCountAccum))

        log.debug("Creating output DF from RDD ...")
        val res = df
          .sparkSession
          .createDataFrame(
            bucketedKvRdd
              .mapPartitions(rows => rows.map(row => row._2), preservesPartitioning = true),
            df.schema
          )
        log.info(s"Repartition done. Number of files, expected: ${bucketedKvRdd.getNumPartitions}")

        res
    }

    log.debug("Repartition done.")
    debugShow(partitionedDF, s"Repartitioned to buckets (rows-per-bucket: ${maxRowsPerBucket}):")
    partitionedDF
  }

  def _repartitionToBucketsSql(
    df: DataFrame,
    partitionsInfo: Option[DataFrameBucketsStats],
    maxRowsPerBucket: Int,
    errorsCountAccum: LongAccumulator
  )(implicit
    log: Logger
  ): Try[DataFrame] = Try {

    val partitionedDF: DataFrame = partitionsInfo match {
      case None => // empty DF
        log.warn("Repartitioning rows to buckets (SQL), dataframe is empty")
        df
      case Some(partsInfo) =>
        log.info("Repartitioning rows to buckets (SQL) ...")

        val bucketNumColName: String =
          s"""${"a" * df.schema.fieldNames.map(_.length).max}_bucketN"""
        val totalBucketsCount: Int =
          partsInfo.dataFramePartitionsStats.values.map(_.bucketsCount).sum

        // N.B. You have to add column instead of using udf in-place while repartitioning:
        // only deterministic udf allowed, and some strange errors could happen if you try to use Random with seed to make it deterministic
        val dfWithBucketNum: DataFrame = df.withColumn(
          bucketNumColName,
          _createBucketingUDF(partsInfo, errorsCountAccum, df.sparkSession).apply(
            sqlfn.concat_ws("/", partsInfo.partitionColumnsNames.map(sqlfn.col): _*)
          )
        )

        // by-range produces more stable files size than hash; mul-by-2 for eliminating strange effects with files doubling
        val res = dfWithBucketNum
          .repartitionByRange(totalBucketsCount * 2, sqlfn.col(bucketNumColName))
          .drop(bucketNumColName)

        log.info(s"Repartition done. Number of files, expected: ${totalBucketsCount}")

        res
    }

    debugShow(partitionedDF, s"Repartitioned to buckets (rows-per-bucket: ${maxRowsPerBucket}):")
    partitionedDF
  }

  def _createBucketingUDF(
    partitionsInfo: DataFrameBucketsStats,
    errorsCountAccum: LongAccumulator,
    spark: SparkSession
  ): UserDefinedFunction = {
    // N.B. Only stdlib types allowed in broadcast because of the `SQL ADD JAR ...` classloader problem, e.g:
    // java.lang.ClassCastException: com...DataFrameHivePartitionsInfo cannot be cast to com...DataFrameHivePartitionsInfo

    // broadcast parameters, tuple of:
    // 1: random
    // 2: buckets_map: (part_id -> (buckets_count, first_bucket))
    // 3: total_buckets_count
    val bcParams = spark
      .sparkContext
      .broadcast(
        (
          new Random(),
          partitionsInfo.dataFramePartitionsStats.map {
            case (k, v) => k -> (v.bucketsCount, v.firstBucket)
          },
          partitionsInfo.dataFramePartitionsStats.values.map(_.bucketsCount).sum
        )
      )

    val getPartBucketNumber: String => Long = (partId: String) => {
      val (numBuckets, firstBucket) = bcParams
        .value
        ._2
        .getOrElse(
          partId, {
            println(
              s"\nGet partition buckets count ERROR, unknown key ${partId}, row goes to a random partition"
            )
            errorsCountAccum.add(1)
            (bcParams.value._3, 0)
          }
        )

      firstBucket + bcParams.value._1.nextInt(numBuckets)
    }

    sqlfn.udf(getPartBucketNumber).asNonNullable().asNondeterministic()
  }

  def alterPartitionsParameters(
    database: String,
    table: String,
    partStats: Option[DataFrameBucketsStats],
    params: Map[String, Option[String]],
    failIfNotFoundPartitions: Boolean = true
  )(implicit
    log: Logger,
    spark: SparkSession
  ): Try[Unit] = Try {

    log.info(
      s"Updating Hive table partitions parameters `${params
          .mkString(", ")}` for ${partStats.map(_.dataFramePartitions.length).getOrElse(0)} partitions ..."
    )

    def _updateData(data: Map[String, String], updates: Map[String, Option[String]])
      : Map[String, String] =
      updates.foldLeft(data) {
        case (result, item) =>
          item match {
            case (key, None) => result - key // remove record
            case (key, Some(value)) => result + ((key, value)) // add record
          }
      }

    partStats match {
      case None => log.warn("Have no partitions to update")
      case Some(stats) =>
        if (params.isEmpty) log.warn("Have no parameters to set")
        else {
          val poolSize = computeConnectionsPoolSize(spark, stats.dataFramePartitions.length)
          log.debug(s"Connections pool size, expected: ${poolSize}")
          val mqpOpt: Option[MetastoreQueryProcessorWithConnPool] =
            MetastoreQueryProcessorWithConnPool(spark, poolSize)
          log.debug(s"Connections pool size, actual: ${mqpOpt.map(_.poolActualSize)}")

          log.info(s"Filtering metastore partitions ...")
          val partitions: Seq[CatalogTablePartition] =
            _listPartitions(database, table, stats.dataFramePartitions, mqpOpt)
          log.info(s"Filtered metastore partitions: ${partitions.length}")

          require(
            partitions.length == stats.dataFramePartitions.length || !failIfNotFoundPartitions,
            s"Number of partitions from metastore must be = ${stats.dataFramePartitions.length}, got (${partitions.length})"
          )

          if (partitions.nonEmpty) {
            log.debug(
              s"Filtered metastore partitions: first 20:\n${partitions.take(20).mkString(";\n")}"
            )
            val counts = partitions.map(p => p.parameters.getOrElse("numFiles", "0").toInt)
            log.debug(
              s"Number of files in partitions (by catalog), list (first 99): [${counts.take(99).mkString(",")}]"
            )
            log.info(s"Number of files in partitions (by catalog), total: ${counts.sum}")

            val updatedParts = partitions.map(p =>
              p.copy(parameters = _updateData(data = p.parameters, updates = params))
            )

            log.info("Updating metastore partitions ...")
            _alterPartitions(database, table, updatedParts, mqpOpt)
            log.info("Partitions parameters updated.")
          }
          else log.info("Have no partitions to update.")

          // close if optional pool exists
          mqpOpt.foreach(_.closeConnections())
        } // params.nonEmpty
      // partStats.nonEmpty
    }
  }

  def _listPartitions(
    database: String,
    table: String,
    parts: Seq[Map[String, String]],
    mqp: Option[MetastoreQueryProcessorWithConnPool]
  )(implicit
    log: Logger,
    spark: SparkSession
  ): Array[CatalogTablePartition] = {
    // split large amount of partitions to chunks to avoid catalyst/metastore hanging or StackOverflow;
    // use an optional pool of connections to speed-up queries processing

    val filters: Array[CatalystExpression] =
      _splitToChunks(parts, PARTITIONS_LIST_READ_CHUNK_SIZE)
        .map(_buildPartitionsFilter)
        .toArray

    val askCatalogFun: (CatalystExpression, ExternalCatalog) => Seq[CatalogTablePartition] =
      (filter, catalog) => {
        log.debug(s"Filter sql: `... ${filter.sql.takeRight(199)}`")
        catalog.listPartitionsByFilter(database, table, Seq(filter), TIME_ZONE_ID)
      }

    _processPartitions(filters, askCatalogFun, mqp).toArray
  }

  def _alterPartitions(
    database: String,
    table: String,
    parts: Seq[CatalogTablePartition],
    mqp: Option[MetastoreQueryProcessorWithConnPool]
  )(implicit
    log: Logger,
    spark: SparkSession
  ): Unit = {
    // split large amount of partitions to chunks to avoid metastore hanging or StackOverflow;
    // use an optional pool of connections to speed-up queries processing

    val chunks: Array[Seq[CatalogTablePartition]] =
      _splitToChunks(parts, PARTITIONS_LIST_WRITE_CHUNK_SIZE).toArray

    val askCatalogFun: (Seq[CatalogTablePartition], ExternalCatalog) => Seq[Unit] =
      (chunk, catalog) => {
        log.debug(s"Chunk first item location: ${chunk.headOption.map(_.location)}")
        Seq(catalog.alterPartitions(database, table, chunk))
      }

    _processPartitions(chunks, askCatalogFun, mqp)
  }

  def _processPartitions[A, B](
    chunks: IndexedSeq[A],
    askCatalogFun: (A, ExternalCatalog) => Seq[B],
    mqpOpt: Option[MetastoreQueryProcessorWithConnPool]
  )(implicit
    log: Logger,
    spark: SparkSession
  ): Seq[B] = {
    assert(chunks.nonEmpty, "Queries list must not be empty")

    // use an optional pool of connections to speed-up queries processing
    mqpOpt.map {qp => {
      log.info(s"Using ${qp.poolActualSize} connections for ${chunks.length} queries")
      qp.processQueries(chunks, askCatalogFun)
    }} getOrElse {
      log.info(s"Using single global externalCatalog connection for ${chunks.length} queries")
      chunks.flatMap(chunk => askCatalogFun(chunk, spark.sharedState.externalCatalog))
    }
  }

  private def _splitToChunks[T](xs: Seq[T], chunkSize: Int): Seq[Seq[T]] =
    xs.sliding(chunkSize, chunkSize).toSeq

  def _buildPartitionsFilter(parts: Seq[Map[String, String]]): CatalystExpression = {
    import org.apache.spark.sql.catalyst.expressions._
    import org.apache.spark.sql.catalyst.dsl.expressions._

    val allPartsPredicates: Seq[Predicate] = parts.map(part =>
      part
        .map {
          case (k, v) =>
            DslSymbol(Symbol(k)).string === v // dsl.expressions._ implicit magic
        }
        .toSeq
        .reduce(And)
    )

    allPartsPredicates.reduce(Or)
  }

  def write(
    df: DataFrame,
    database: String,
    table: String,
    overwrite: Boolean
  )(implicit
    log: Logger
  ): Try[Unit] = {
    log.info("Writing dataframe to table ...")
    val spark = df.sparkSession

    val savedMode = spark.conf.get(PARTITION_OVERWRITE_MODE_KEY, default = "dynamic")
    spark.conf.set(PARTITION_OVERWRITE_MODE_KEY, "dynamic")

    val resultOrFailure = Try {
      if (DEBUG_MODE) df.explain(extended = true)

      // beware, if partitions.count more than 10K, you may need to increase `spark.hadoop.hive.metastore.client.socket.timeout`
      df.write
        .mode(
          if (overwrite) SaveMode.Overwrite else SaveMode.Append
        )
        .insertInto(s"${database}.${table}")

      log.info("Dataframe successfully written.")
    }

    spark.conf.set(PARTITION_OVERWRITE_MODE_KEY, savedMode)
    resultOrFailure
  }

  class BucketPartitioner(
    parameters: Broadcast[(Random, Int, Map[String, (Int, Int)])],
    errorsAccum: LongAccumulator
  ) extends Partitioner {
    // parameters:
    // 1: random
    // 2: num_partitions = dataFramePartitionsStats.values.map(_.bucketsCount).sum
    // 3: buckets_map: (part_id -> (first_bucket, buckets_count))
    @inline private def rnd = parameters.value._1
    @inline private def bucketsInfo = parameters.value._3

    override def numPartitions: Int = parameters.value._2

    override def getPartition(key: Any): Int = key match {
      // 0 .. numPartitions - 1
      case partitionID: String =>
        bucketsInfo
          .get(partitionID)
          .map { case (firstBucket, bucketsCount) => firstBucket + rnd.nextInt(bucketsCount) }
          .getOrElse {
            println(
              s"\nBucketPartitioner.getPartition ERROR, unknown key ${repr(key)}, row goes to a random partition"
            )
            errorsAccum.add(1)
            rnd.nextInt(numPartitions)
          }
      case _ =>
        println(
          s"\nBucketPartitioner.getPartition ERROR, unknown non-string key ${repr(key)}, row goes to a random partition"
        )
        errorsAccum.add(1)
        rnd.nextInt(numPartitions)
    }
  }

  val PARTITION_OVERWRITE_MODE_KEY: String = "spark.sql.sources.partitionOverwriteMode"
  val TIME_ZONE_ID: String = "UTC" // timezone id to parse partition values of TimestampType, for ExternalCatalog

  val RESERVED_PART_PARAM_NAMES: Set[String] = Set(
    "rawDataSize",
    "numFiles",
    "numFilesErasureCoded",
    "transient_lastDdlTime",
    "totalSize",
    "spark.sql.statistics.totalSize",
    "COLUMN_STATS_ACCURATE",
    "numRows"
  )

  val SQL_PARTITIONER: String = "SQL"
  val RDD_PARTITIONER: String = "RDD"
  private val DEFAULT_PARTITIONER = SQL_PARTITIONER

  // Hive metastore queries pooling/batching
  // N.B. setting `hive.metastore.client.socket.timeout` in runtime may have no effect

  val PARTITIONS_LIST_READ_CHUNK_SIZE: Int = 150
  val PARTITIONS_LIST_WRITE_CHUNK_SIZE: Int = 500

  val METASTORE_CONNECTIONS_POOL_SIZE_KEY: String = "spark.hadoop.vasnake.metastore.pool.size"
  val METASTORE_CONNECTIONS_POOL_SIZE_DEFAULT: Int = 8

  def computeConnectionsPoolSize(spark: SparkSession, partitionsTotalCount: Int): Int = {
    // return min(config.pool_size, partitions.chunks_count)
    // N.B. actual pool size could be different

    val param = math.max(
      1,
      _poolSizeParameterValue(
        spark,
        METASTORE_CONNECTIONS_POOL_SIZE_KEY,
        METASTORE_CONNECTIONS_POOL_SIZE_DEFAULT
      )
    )

    _poolSize(
      maxSize = param,
      partitionsTotalCount = partitionsTotalCount,
      chunkSize = math.min(PARTITIONS_LIST_READ_CHUNK_SIZE, PARTITIONS_LIST_WRITE_CHUNK_SIZE)
    )
  }

  private def _poolSizeParameterValue(
    spark: SparkSession,
    key: String,
    default: Int
  ) = Try(
    spark.conf.get(key, default.toString).toInt
  ).getOrElse(default)

  private def _poolSize(
    maxSize: Int,
    partitionsTotalCount: Int,
    chunkSize: Int
  ) = {
    assert(maxSize > 0, "Pool size must be at least 1")
    val chunksCount = math.max(
      1,
      math.ceil(partitionsTotalCount.toDouble / chunkSize.toDouble).toInt
    )

    math.min(maxSize, chunksCount)
  }

  // debug tools

  def debugShow(
    df: => DataFrame,
    message: => String = "",
    nrows: Int = 200,
    truncate: Boolean = false,
    force: Boolean = false
  ): Unit =
    if (DEBUG_MODE || force) {
      if (message.nonEmpty)
        println(s"\n${message} rows: ${df.count()}; partitions: ${df.rdd.getNumPartitions}\n")
      df.printSchema()
      df.show(numRows = nrows, truncate = truncate)
    }

  val DEBUG_MODE: Boolean = false
}
// TODO: refactor, make it readable; de-couple modules (parameters, stats, metastore, repartition, write)
