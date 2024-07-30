/**
 * Created by vasnake@gmail.com on 2024-07-30
 */
package com.github.vasnake.spark.app.datasets

import org.apache.spark.broadcast.Broadcast

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql

import scala.util.{Failure, Success, Try}
import scala.collection.mutable
import org.apache.log4j.Logger

import org.json4s

import com.github.vasnake.spark.app.datasets.joiner._
import com.github.vasnake.common.file.FileToolbox
import com.github.vasnake.spark.app.SparkSubmitApp
import com.github.vasnake.spark.app.datasets.joiner._
import com.github.vasnake.core.text.StringToolbox
import com.github.vasnake.spark.io.{CheckpointService, IntervalCheckpointService, hive}
import com.github.vasnake.text.parser.JoinExpressionParser
import com.github.vasnake.text.evaluator._
import com.github.vasnake.spark.dataset.transform.Joiner.JoinRule
import com.github.vasnake.spark.features.aggregate.DatasetAggregator
import DatasetAggregator.DatasetAggregators
import com.github.vasnake.`etl-core`.aggregate

object EtlFeatures {
  import implicits._

  val HDFS_TMP_DIR_KEY = "spark.hadoop.vasnake.hdfs.tmp.dir"
  val HDFS_TMP_DIR_DEFAULT = "hdfs:/tmp/vasnake__JoinerApp_default_tmp_dir"

  val MIN_TARGET_ROWS_MIN = 1
  val MIN_TARGET_ROWS_DEFAULT = 1

  val CHECKPOINT_INTERVAL_MIN = 5
  val CHECKPOINT_INTERVAL_DEFAULT = 10

  type DomainAggregationConfig = Map[String, AggregationConfig]
  def keyColumns: Seq[String] = Seq(DT_COL_NAME, UID_COL_NAME, UID_TYPE_COL_NAME)
  def uidKeyPair: Seq[String] = Seq(UID_COL_NAME, UID_TYPE_COL_NAME)
  val SNB_DB_PREFIXES: Seq[String] = Seq("snb_", "sandbox_")
  val DOMAIN_AGG_KEY = "domain"

  object DomainType {
    val MAP_TYPE = "MAP_TYPE"
    val ARRAY_TYPE = "ARRAY_TYPE"
    val PREFIX_TYPE = "PREFIX_TYPE"
  }

  // if no config for domain: use default drop-null-avg config
  @transient
  val defaultAggregationConfig: DomainAggregationConfig = Map(DOMAIN_AGG_KEY -> AggregationConfig(
    pipeline = List("agg"),
    stages = Map("agg" -> AggregationStageConfig(name = "avg", kind = "agg", parameters = Map.empty))
  ))

  def resultWithLog[T](job: => T, log: => Unit): T = {
    val res = job
    log
    res
  }

  def escalateError[T](t: Try[T], log: Throwable => Unit): T = t match {
    case Failure(exception) => {
      log(exception) // n.b. to perform exception.printStackTrace() pass such a log that calls logger.error(msg, exception)
      throw exception
    }
    case Success(v) => v
  }

  def splitTableName(fqTableName: String): (String, String) = fqTableName.split('.').toList match {
    case db :: table :: Nil => (db, table)
    case _ => sys.error(s"Malformed table full name `${fqTableName}`, must be in form of `dbname.tablename`")
  }

  def setParallelism(numPartitions: Int)(implicit spark: SparkSession): Unit = {
    spark.conf.set("spark.default.parallelism", numPartitions)
    spark.conf.set("spark.sql.shuffle.partitions", numPartitions)
  }

  @transient private val memo: mutable.Map[String, String] = mutable.Map.empty[String, String]
  private def columnsSizes(domain: String, cols: Seq[String], df: DataFrame): Map[String, Int] = {
    // TODO: consider optimization possibilities: catalog, pre-calculated stats, analyze table and such
    //  https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-cost-based-optimization.html

    def memoKey(colname: String) = s"${domain}.${colname}.size"

    val colsToCompute = cols.filter(n => !memo.contains(memoKey(n)))
    if (colsToCompute.nonEmpty) {
      val exprs = colsToCompute.map(n => s"max(size(${n})) as $n")

      // action
      val row = df.selectExpr(exprs: _*).head()
      // empty df generate null values; not empty df but null columns generate `-1` values.
      // null values from empty df saved as `0` size; null columns from non-empty df saved as `-1` size.

      colsToCompute.foreach { colName =>
        val colIdx = row.fieldIndex(colName)
        if (row.isNullAt(colIdx)) memo.update(memoKey(colName), "0")
        else memo.update(memoKey(colName), row.getInt(colIdx).toString)
      }
    }

    cols.map(n => (n, memo.getOrElse(memoKey(n), "-1").toInt)).toMap
  }

  trait DomainBuilder {
    import sql.types.DataType

    def collectionClass: Class[_]
    def collectionDataType: DataType
    def collectPrimitives(df: DataFrame, cols: Seq[String], domainName: String): DataFrame
    def renameToDomain(df: DataFrame, col: String, domainName: String): DataFrame
    def mergeCollections(df: DataFrame, domainName: String, cols: Seq[String]): DataFrame
    def notNullItemsCount(domainName: String): sql.Column
  }

  class ArrayDomainBuilder(itemDataType: sql.types.DataType) extends DomainBuilder {
    import sql.types.{ArrayType, DataType}
    import sql.{functions => sf}

    def collectionClass: Class[_] = classOf[ArrayType]

    def collectionDataType: DataType = ArrayType(itemDataType)

    def collectPrimitives(df: DataFrame, cols: Seq[String], domainName: String): DataFrame = {
      // source.withColumn(s"${domainName}_primitives", ...) produces wrong columns order
      val domainColumn = sf.array(cols.map(cn => sf.col(cn).cast(itemDataType)): _*)
      df.select(domainColumn.as(s"${domainName}_primitives") +: df.columns.map(sf.col): _*)
    }

    def renameToDomain(df: DataFrame, col: String, domainName: String): DataFrame = df.withColumnRenamed(col, domainName)

    def mergeCollections(df: DataFrame, domainName: String, cols: Seq[String]): DataFrame = {
      // what should I do when `$arraycolumn is null`: replace missing features with null values
      lazy val sizes: Map[String, Int] = {
        val res = columnsSizes(domainName, cols, df)
        require(
          res.values.forall(sz => sz >= 0),
          s"ARRAY_TYPE domain parts must have size >= 0. Domain `${domainName}`, parts sizes `${res}`"
        )
        res
      }

      df.withColumn(domainName, sf.concat(cols.map(n =>
        sf.coalesce(
          sf.col(n),
          sf.expr(s"array_repeat(cast(null as float), ${sizes(n)})")
        )): _*))
    }

    def notNullItemsCount(domainName: String): sql.Column =
      sf.size(sf.expr(s"filter(coalesce(${domainName}, array()), _x -> _x IS NOT NULL)"))

  }

  class MapDomainBuilder(itemDataType: sql.types.DataType) extends DomainBuilder {
    import sql.types.{MapType, DataType, StringType}
    import sql.{functions => sf}

    def collectionClass: Class[_] = classOf[MapType]

    def collectionDataType: DataType = MapType(StringType, itemDataType)

    def collectPrimitives(df: DataFrame, cols: Seq[String], domainName: String): DataFrame = {
      val domainColumn = sf.map(
        cols.map(sf.lit).zip( // (name, column)
          cols.map(n => sf.col(n).cast(itemDataType))
        ).flatMap { // name, column, name, column, ...
          case (k, v) => Array(k, v)
        } : _*
      )

      df.select(domainColumn.as(s"${domainName}_primitives") +: df.columns.map(sf.col): _*)
    }

    def renameToDomain(df: DataFrame, col: String, domainName: String): DataFrame = {
      dropMapNullValues(
        df.withColumnRenamed(col, domainName),
        domainName
      )
    }

    def mergeCollections(df: DataFrame, domainName: String, cols: Seq[String]): DataFrame = {
      cols.foldLeft(df)((df, colName) =>
        dropMapNullValues(df, colName, Some(collectionDataType))
      )
        .withColumn(
          domainName,
          sf.expr(s"user_dmdesc.combine(${cols.mkString(",")})")
        )
    }

    def notNullItemsCount(domainName: String): sql.Column =
      sf.size(
        sf.coalesce(
          sf.col(domainName),
          sf.expr("map()")
        )
      )

  }

  def dropMapNullValues(df: DataFrame, colName: String, colType: Option[sql.types.DataType] = None): DataFrame = {
    import sql.{functions => sf}

    val expr = {
      // I'm sure that keys.order is the same as values.order
      // https://github.com/apache/spark/blob/7ab167a9952c363306f0b9ee7402482072039d2b/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/ArrayBasedMapData.scala#L28
      val _expr = sf.expr(
        s"map_from_entries(filter(arrays_zip(map_keys(${colName}), map_values(${colName})), _x -> _x['1'] is not null))"
      )

      colType.map(_expr.cast)
        .getOrElse(_expr)
    }

    df.withColumn(colName, expr)
  }

  def parseJsonEtlConfig(cfg: String): EtlConfig = {
    import json4s._
    import json4s.jackson.JsonMethods._
    implicit val formats: DefaultFormats = DefaultFormats

    parse(cfg).extract[EtlConfig]
  }

  def sourcesFromConfig(cfg: ISourcesConfigView): Seq[DomainSourceDataFrame] = {
    // consider optimizations for a case: few domains using one source
    for {
      domain <- cfg.domains
      source <- cfg.sources(domain)
    } yield DomainSourceDataFrame(domain, source, cfg.table(source), None)
  }

  def tableExists(tableName: String)(implicit spark: SparkSession): Boolean = {
    spark.catalog.tableExists(tableName)
  }

  def addMissingColumns(table: DataFrame, targetSchema: StructType): DataFrame =
    targetSchema.foldLeft(table){ case (df, field) =>
      if (table.columns.contains(field.name)) df
      else df.withColumn(field.name, sql.functions.lit(null).cast(field.dataType))
    }

  def createEmptyTable(df: DataFrame, fqTableName: String, partitionColumnsNames: Seq[String])(implicit log: Logger): Unit = {
    val writer = hive.SQLWriterFactoryImpl.getWriter(new hive.WriterConfig { def spark: Option[SparkSession] = Some(df.sparkSession) })

    val res: Try[Unit] = for {
      (db, table) <- Try(splitTableName(fqTableName))
      ddl <- writer.createTableDDL(db, table, partitionColumnsNames, df.schema)
      _ <- writer.execHiveDDL(ddl)
    } yield ()

    escalateError(res, exception => log.fatal(s"Create table failed: ${exception.getMessage}", exception))
  }

  def writePartitionToTable(df: DataFrame, fqTableName: String, partition: Map[String, String])(implicit log: Logger): Unit = {
    val writer = hive.SQLWriterFactoryImpl.getWriter(new hive.WriterConfig { def spark: Option[SparkSession] = Some(df.sparkSession) })

    val res: Try[Unit] = for {
      (db, table) <- Try(splitTableName(fqTableName))
      dml <- writer.insertStaticPartDML(db, table, partition, df)
      _ <- writer.execHiveDML(dml)
    } yield ()

    escalateError(res, exception => log.fatal(s"Write to table failed: ${exception.getMessage}", exception))
  }

  def writeToStagingCheckRowsCountAndRepartition(
                                                  df: DataFrame,
                                                  stagingHdfsDir: String,
                                                  minTargetRows: Long,
                                                  outputPartitions: Int
                                                )(
                                                  implicit log: Logger
                                                ): Try[DataFrame] = Try {

    log.info(s"Writing result to staging dir: `${stagingHdfsDir}` ...")

    implicit val spark: SparkSession = df.sparkSession

    val repartitionedDF = repartitionToOutputParts(df, outputPartitions)

    repartitionedDF.write
      .mode("overwrite")
      .option("compression", "gzip")
      .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
      .parquet(stagingHdfsDir)

    setParallelism(outputPartitions)

    val persistedDF = spark.read.parquet(stagingHdfsDir).cache()

    val rowsCount = persistedDF.count()
    require(rowsCount >= minTargetRows, s"Result rows count must be not less than $minTargetRows, got $rowsCount")
    log.info(s"Result rows count: $rowsCount")

    repartitionToOutputParts(persistedDF, outputPartitions)
  }

  def repartitionToOutputParts(df: DataFrame, outputPartitions: Int)(implicit log: Logger): DataFrame = {
    if (df.rdd.getNumPartitions > outputPartitions) {
      log.info(s"Repartition from ${df.rdd.getNumPartitions} to ${outputPartitions} partitions ...")
      df.repartition(outputPartitions, sql.functions.col(UID_COL_NAME))
    } else df
  }

  def parseJoinRule(rule: String, defaultItem: String): JoinExpressionEvaluator[String] = {
    // join rule: "topics full_outer (profs left_outer groups)"
    // or "" or "topics_composed"
    import StringToolbox._

    rule.splitTrim(Separators(" ")).toSeq match {
      case Seq() => SingleItemJoin(defaultItem.trim)
      case Seq(item) => SingleItemJoin(item)
      case Seq(a, b) => throw new IllegalArgumentException(s"Invalid config: malformed join rule `${rule}`")
      case _  => JoinRule.parse(rule)
    }
  }

  def selectDomainsSources(allSources: Seq[DomainSourceDataFrame], domains: Seq[String]): Seq[DomainSourceDataFrame] = {
    allSources.filter(s => domains.contains(s.domain))
  }

  def loadTables(sources: Seq[DomainSourceDataFrame])(implicit spark: SparkSession): Seq[DomainSourceDataFrame] = {
    def loadDF(name: String): Option[DataFrame] = Try { spark.read.table(name) }.toOption

    //sources.map(s => s.copy(df = loadDF(s.table.name)))
    for {
      s <- sources
    } yield s.copy(df = loadDF(s.table.name))
  }

  def prepareSource(source: DomainSourceDataFrame, config: TableConfig): DomainSourceDataFrame = {
    // filter and project table according to config

    // prune dt partitions
    // filter other partitions
    // apply `where` expr
    // fake uid
    // prune uid_type partitions
    // drop invalid OKID, VKID uid records
    // select features
    // drop partitioning columns
    // cast uid to string
    val res = source.df.map(
      _.filterDTPartition(config.dt)
        .filterPartitions(config.partitions)
        .optionalWhere(config.where)
        .imitateUID(config.uid_imitation)
        .filterUidTypePartitions(config.expected_uid_types.map(lst => if (lst.isEmpty) List("") else lst))
        .dropInvalidUID
        .selectFeatures(config.features)
        .dropPartitioningCols(config.partitions, except = Set(UID_TYPE_COL_NAME))
        .castColumnTo(UID_COL_NAME, sql.types.StringType)
    )

    source.copy(df = res)
  }

  def loadMatchingTable(cfg: TableConfig)(implicit spark: SparkSession): DataFrame = spark.read.table(cfg.name)

  def prepareMatchingTable(cfg: TableConfig, df: DataFrame)(implicit spark: SparkSession): Dataset[MatchingTableRow] = {
    import spark.implicits._

    df.filterDTPartition(cfg.dt)
      .filterPartitions(cfg.partitions)
      .optionalWhere(cfg.where)
      .selectExpr(
        "cast(uid1 as string) uid1",
        "cast(uid2 as string) uid2",
        "cast(uid1_type as string) uid1_type",
        "cast(uid2_type as string) uid2_type"
      ).na.drop()
      .as[MatchingTableRow]
  }

  def makeDomainSource(cfg: DomainConfig, sources: Seq[DomainSourceDataFrame]): DomainSourceDataFrame = {
    // join set of sources to one, project domain source according to config

    // "names":     [
    //      "foo as a",
    //      "bar as b",
    //      "baz"
    //    ],
    // "join_rule": "a full_outer (b left_outer baz)"
    // "features":  ["a.*", "b.*", "baz.price as fixed_price"],

    require(
      sources.length == cfg.source.names.length,
      s"Domain sources and config.source.names must be collections of the same size. cfg: ${cfg}, sources: ${sources}"
    )

    val srcWrapper = sources.head

    def joinSources(): Option[DataFrame] = {
      // TODO: require join-rule.items is a subset of sources
      val rule = for (jr <- cfg.source.join_rule) yield parseJoinRule(jr, defaultItem = srcWrapper.source.alias)
      val tables: Map[String, DataFrame] = sources.map(src => (src.source.alias, src.df.get)).toMap

      for (je <- rule) yield joinWithAliases(tables, je)
    }

    val df = if (cfg.source.names.length > 1 && cfg.source.join_rule.getOrElse("").trim.nonEmpty)
      joinSources()
    else srcWrapper.df.map(_.as(srcWrapper.source.alias))

    srcWrapper.copy(df = df.map(_.selectFeatures(cfg.features)))
  }

  def joinWithAliases(tables: Map[String, DataFrame], joinTree: JoinExpressionEvaluator[String]): DataFrame = {
    // join-result = (join-tree, df-catalog) => df(uid, uid_type, features: _*)
    JoinRule.join(
      tree = joinTree,
      catalog = name => tables(name).as(name),
      keys = uidKeyPair
    )
  }

  def joinDomains(
                   domains: Map[String, DataFrame],
                   joinRule: JoinExpressionEvaluator[String],
                   checkpointService: Option[CheckpointService] = None
                 ): DataFrame = {
    val checkPointFun: Option[DataFrame => DataFrame] =
      checkpointService.map(cpService =>
        df => cpService.checkpoint(df)
      )

    JoinRule.join(joinRule, domains, uidKeyPair, checkPointFun)
  }

  def buildDomain(cfg: DomainConfig, source: DataFrame): DataFrame = {
    // collect features to plain|map|array features structure
    val castType = cfg.cast_type.getOrElse("float")

    cfg.group_type.getOrElse(DomainType.MAP_TYPE) match {
      case DomainType.PREFIX_TYPE =>  buildPrefixDomain(cfg.name, source, castType)
      case DomainType.ARRAY_TYPE =>   buildArrayDomain(cfg.name, source, castType)
      case _ =>                       buildMapDomain(cfg.name, source, castType)
    }
  }

  def buildPrefixDomain(domainName: String, source: DataFrame, castType: String): DataFrame = {
    // rename features columns
    import sql.functions.col
    def prefix(colname: String) = s"${domainName}_${colname}"

    val featuresCols = source.columns.filter(!keyColumns.contains(_))

    val domain = featuresCols.foldLeft(source)((df, colname) =>
      df.withColumnRenamed(colname, prefix(colname)))

    // cast features
    domain.select(
      col(UID_COL_NAME) +: col(UID_TYPE_COL_NAME) +:
        featuresCols.map(n => col(prefix(n)).cast(castType))
        : _*)
  }

  def buildArrayDomain(domainName: String, source: DataFrame, castType: String): DataFrame = {
    buildCollectionDomain(domainName, source, new ArrayDomainBuilder(sql.types.DataType.fromDDL(castType)))
  }

  def buildMapDomain(domainName: String, source: DataFrame, castType: String): DataFrame = {
    buildCollectionDomain(domainName, source, new MapDomainBuilder(sql.types.DataType.fromDDL(castType)))
  }

  def buildCollectionDomain(domainName: String, source: DataFrame, builder: DomainBuilder): DataFrame = {
    // collect primitive columns to collection; add merged column:
    // if have primitives: add collection column
    // if complex.length == 1: rename to domain
    // if complex.length > 1: add merged collections

    import sql.types.{ArrayType, MapType, DataType, StructType}
    import sql.{functions => sf}

    // collect primitives to collectionColumn
    val df: DataFrame = {
      val complexTypes: Seq[Class[_]] = Seq(classOf[ArrayType], classOf[MapType], classOf[StructType])
      val primitiveColNames: Seq[String] = source.schema.filter(fld => !(
        keyColumns.contains(fld.name) || complexTypes.contains(fld.dataType.getClass)
        )).map(_.name)

      if (primitiveColNames.nonEmpty)
        builder.collectPrimitives(source, primitiveColNames, domainName)
      else
        source
    }

    val collectionTypeColNames: Seq[String] = df.schema.filter(fld =>
      !keyColumns.contains(fld.name) && fld.dataType.getClass == builder.collectionClass
    ).map(_.name)

    // create domain column
    val res = if (collectionTypeColNames.length < 1)
      df.withColumn(domainName, sf.lit(null))
    else if (collectionTypeColNames.length == 1)
      builder.renameToDomain(df, collectionTypeColNames.head, domainName)
    else
      builder.mergeCollections(df, domainName, collectionTypeColNames)

    res.select(
      sf.col(UID_COL_NAME),
      sf.col(UID_TYPE_COL_NAME),
      sf.when(builder.notNullItemsCount(domainName) < 1, null)
        .otherwise(res(domainName))
        .cast(builder.collectionDataType)
        .alias(domainName)
    )
  }

  def mapUids(records: DataFrame, mtable: Dataset[MatchingTableRow], inUidTypes: Seq[String], outUidType: String)
             (cache: DataFrame => DataFrame): DataFrame = {
    // map input uids to output using cross-table
    import sql.{functions => sf}

    // three options available:
    // 1 - mapping to different types, e.g. OKID, EMAIL => VKID
    // 2 - mapping to self, e.g. OKID, VKID => VKID, and cross-table doesn't contain self-mapping pairs:
    //    must retain original VKID records
    // 3 - mapping to self, e.g. OKID, VKID => VKID, and cross-table contain self-mapping pairs:
    //    must drop original VKID records duplicates

    // TODO: use constants and MatchingTableRow.names in sql code

    def innerJoinMap(df: DataFrame, cross: Dataset[MatchingTableRow]): DataFrame = {
      val mapped = df.as("a").join(
        cross.as("b"),
        sf.expr("a.uid_type = b.uid1_type and a.uid = b.uid1"),
        "inner"
      )

      mapped.selectExpr("a.*", "b.uid2")
        .drop("uid", "uid_type")
        .withColumnRenamed("uid2", "uid")
    }

    // TODO: remove uid_type from result -- it's a constant given in job parameter
    def restoreSchema(df: DataFrame): DataFrame = df.withColumn("uid_type", sf.lit(outUidType))
      .setColumnsInOrder(withDT = false, withUT = true)

    def withSelfMapping: DataFrame = {
      val cross = mtable.where(
        sf.not(sf.expr(s"uid2_type = uid1_type and uid1 = uid2"))
      )  // making sure there is no loops

      val cachedRecs = cache(records)
      val mapped = innerJoinMap(cachedRecs, cross)
      val selfMapped = cachedRecs.where(s"uid_type = '${outUidType}'").drop("uid_type")
      val united = mapped.unionByName(selfMapped)

      restoreSchema(united)
    }

    def woSelfMapping: DataFrame = restoreSchema(
      innerJoinMap(records, mtable)
    )

    if (inUidTypes.contains(outUidType)) withSelfMapping
    else woSelfMapping
  }

  def aggregateDomains(df: DataFrame, cfg: Map[String, DomainAggregationConfig])(implicit spark: SparkSession): DataFrame = {
    // compile aggregation functions for each domain (may be for features in domain), perform aggregation
    import spark.implicits._
    import org.apache.spark.sql.catalyst.encoders.RowEncoder
    implicit val outRowEncoder: ExpressionEncoder[Row] = RowEncoder(df.schema)

    val aggregators: Broadcast[DatasetAggregators] = spark.sparkContext.broadcast(
      compileAggregators(cfg, df)
    )

    df.groupByKey(row => row.getAs[String](UID_COL_NAME))
      .mapGroups { case (key, rows) => reduceRows(key, rows, aggregators) }
  }

  def reduceRows(key: String, iter: Iterator[Row], aggregators: Broadcast[DatasetAggregators]): Row = {
    // apply agg to each column (uid is a special case)
    val aggs = aggregators.value
    val rows: Seq[Row] = iter.toSeq

    val aggregatedColumns = for {
      aggregator <- aggs.columnsAgg
    } yield aggregator.apply(rows)

    Row.fromSeq(aggregatedColumns)
  }

  def compileAggregators(cfg: Map[String, DomainAggregationConfig], df: DataFrame): DatasetAggregators = {
    val datasetAggCfg: Map[String, DatasetAggregator.ColumnsAggregationConfig] =
      cfg.map { case (name, cfg) => (name, convertAggConfig(cfg)) }

    DatasetAggregator.compileAggregators(datasetAggCfg, df)
  }

  def convertAggConfig(datasetAggConfig: DomainAggregationConfig): DatasetAggregator.ColumnsAggregationConfig = {
    import aggregate.config.{AggregationPipelineConfig, AggregationStageConfig}

    datasetAggConfig map { case (name, pipelineCfg) => (name, AggregationPipelineConfig(
      pipeline = pipelineCfg.pipeline,
      stages = pipelineCfg.stages map { case (name, stageCgf) => (name, AggregationStageConfig(
        name = stageCgf.name,
        kind = stageCgf.kind,
        parameters = stageCgf.parameters
      ))}
    ))}
  }

}
