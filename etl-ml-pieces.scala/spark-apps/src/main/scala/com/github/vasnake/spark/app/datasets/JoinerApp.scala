/**
 * Created by vasnake@gmail.com on 2024-07-30
 */
package com.github.vasnake.spark.app.datasets

//import scala.collection.mutable
//import org.apache.log4j.Logger
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
//import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql

import scala.util.{Failure, Success, Try}
import com.beust.jcommander

import com.github.vasnake.common.file.FileToolbox
import com.github.vasnake.spark.app.SparkSubmitApp
import com.github.vasnake.spark.app.datasets.joiner._
import com.github.vasnake.core.text.StringToolbox
import com.github.vasnake.spark.io.{CheckpointService, IntervalCheckpointService, hive}
import com.github.vasnake.text.parser.JoinExpressionParser
import com.github.vasnake.text.evaluator._
import com.github.vasnake.spark.dataset.transform.Joiner.JoinRule

/**
 * App stages:
 * - load and parse job config;
 * - load source DFs;
 * - transform sources to features domains;
 * - join domains, produce output partition;
 * - write if no matching required, or:
 * - do matching stages;
 * - write.
 */
object JoinerApp extends SparkSubmitApp(CmdLineParams) {
  import EtlFeatures._

  logger.info(s"Loading ETL config `${CmdLineParams.etl_config}` ...")
  private val etlCfg: EtlConfig = {
    import StringToolbox._
    import DefaultB64Mapping.extraCharsMapping

    val b64decoded = CmdLineParams.etl_config.b64Decode
    logger.info(s"config json: `${b64decoded}`")
    parseJsonEtlConfig(b64decoded)
  }
  logger.info(s"EtlConfig loaded: `${etlCfg}`")

  private val minTargetRows: Long = math.max(CmdLineParams.min_target_rows, MIN_TARGET_ROWS_MIN)
  logger.info(s"Parameter min_target_rows, effective: ${minTargetRows}")

  private val checkpointInterval: Int = math.max(CmdLineParams.checkpoint_interval, CHECKPOINT_INTERVAL_MIN)
  logger.info(s"Parameter checkpoint_interval, effective: ${checkpointInterval}")

  private val stagingHdfsDir: String = buildTempPath("staging")
  logger.info(s"Parameter staging_hdfs_dir, effective: `${stagingHdfsDir}`")

  private val checkpointHdfsDir: String = buildTempPath("checkpoint")
  logger.info(s"Parameter checkpoint_hdfs_dir, effective: `${checkpointHdfsDir}`")

  setParallelism(etlCfg.shuffle_partitions)

  private val allDomainsSources = findSourceTables()
  logger.info(s"Sources enumerated: `${allDomainsSources.mkString(";\n")}`")

  private val needMatching: Boolean = {
    // check config, do we need matching?
    etlCfg.matching.nonEmpty
  }

  if (!tableExists(etlCfg.table)) {
    logger.warn(s"Target table `${etlCfg.table}` doesn't exists ...")
    createTargetTable()
    logger.info("empty table created.")
  }

  val targetDF = {
    val joinedDF = joinDomainsWithCheckpoints(partitionJoinRule)
    logger.info(s"Joined domains: ${joinedDF.schema.mkString(";")}")

    if (needMatching) {
      logger.info(s"UID mapping (with aggregation) required ...")
      val res = doMatching(joinedDF)
      logger.info(s"Matching done, ${res.schema.mkString(";")}")
      res
    } else joinedDF
  }

  saveResult(targetDF)
  logger.info("Done.")

  private def buildTempPath(prefix: String): String =
    FileToolbox.joinPath(
      getSparkConfParameterValue(HDFS_TMP_DIR_KEY, HDFS_TMP_DIR_DEFAULT),
      s"${prefix}__${etlCfg.table}__dt=${etlCfg.dt}__uid_type=${etlCfg.uid_type}"
    )

  private def createTargetTable(): Unit = {
    // use table join rule to select domains and their order
    // load domain sources
    // transform sources: for complex domains: build one domain source DF using domain join rule
    // transform each domain source into domain DF, collect features
    // join domains
    // use result schema to create table

    if (!SNB_DB_PREFIXES.exists(prefix => etlCfg.table.startsWith(prefix))) {
      val msg = s"Table creation allowed only for db names with prefixes `${SNB_DB_PREFIXES.mkString(",")}`."
      logger.error(msg)
      throw new UnsupportedOperationException(msg)
    }

    val target: DataFrame = joinDomainsNoCheckpoints(tableJoinRule)
    logger.info(s"Joined domains: ${target.schema.mkString(";")}")

    createEmptyTable(target)
  }

  private def createEmptyTable(result: DataFrame): Unit = {
    import implicits._

    val df = addDtColumn(result).setColumnsInOrder(withDT = true, withUT = true)
    val fqTableName = etlCfg.table

    logger.info(s"Creating empty table `${fqTableName}` `${df.schema.mkString(";")}` ...")
    EtlFeatures.createEmptyTable(df, fqTableName, partitionColumnsNames = Seq(DT_COL_NAME, UID_TYPE_COL_NAME))
  }

  private def saveResult(df: DataFrame): Unit = {
    // write result partition to Hive table
    import implicits._

    val fqTableName = etlCfg.table // fully qualified table name
    val partition = Map(DT_COL_NAME -> etlCfg.dt, UID_TYPE_COL_NAME -> etlCfg.uid_type)

    logger.info(s"Saving result: `${df.schema.mkString(";")}` to `$fqTableName/$partition` ...")

    val res = for {
      persistedDF <- writeToStagingCheckRowsCountAndRepartition(df, stagingHdfsDir, minTargetRows, etlCfg.getWritePartitions)
      fixedDF <- Try(addMissingColumns(persistedDF, spark.read.table(fqTableName).schema)) // it's fast, shouldn't worry about speed
      _ <- Try(EtlFeatures.writePartitionToTable(fixedDF, fqTableName, partition))
    } yield ()

    escalateError(res, exception => logger.fatal(s"Saving result failed: ${exception.getMessage}", exception))
  }

  // TODO: check for special cases: (join_rule is empty or contains single name) and domains list size == 1
  private def tableJoinRule = parseJoinRule(
    etlCfg.table_join_rule.getOrElse("").trim,
    etlCfg.domains.head.name
  )

  private def partitionJoinRule = parseJoinRule(
    etlCfg.join_rule.getOrElse(
      etlCfg.table_join_rule.getOrElse("")
    ).trim,
    etlCfg.domains.head.name
  )

  private def joinDomainsNoCheckpoints(joinRule: JoinExpressionEvaluator[String]): DataFrame = {
    // use table join rule; see `createTargetTable`
    joinDomains(joinRule, checkpointService = None)
  }

  private def joinDomainsWithCheckpoints(joinRule: JoinExpressionEvaluator[String]): DataFrame = {
    // use partition join rule to join domains; see `targetDF`
    joinDomains(
      joinRule,
      checkpointService = Some(new IntervalCheckpointService(checkpointInterval, checkpointHdfsDir))
    )
  }

  private def joinDomains(joinRule: JoinExpressionEvaluator[String], checkpointService: Option[CheckpointService]): DataFrame = {
    // select domains and their order
    // load domain sources
    // transform sources: for complex domains: build one domain source DF using domain join rule
    // transform each domain source into domain DF, collect features
    // join domains
    logger.info(s"Join domains rule: ${joinRule}")

    val domainsNames: Seq[String] = JoinRule.enumerateItems(joinRule)
    val sources: Seq[DomainSourceDataFrame] = selectDomainsSources(allDomainsSources, domainsNames)

    val sourcesWithDF: Seq[DomainSourceDataFrame] = loadTables(sources)
    require(sourcesWithDF.forall(s => s.df.nonEmpty), "All required tables should be accessible")
    sourcesWithDF.foreach(s => logger.info(s"Source `${s.source}` table: ${s.df.get.schema.mkString(";")}"))

    val domainsSources: Seq[DomainSourceDataFrame] = makeDomainSources(domainsNames, sourcesWithDF)
    domainsSources.foreach(source => logger.info(s"Domain `${source.domain}` source: ${source.df.get.schema.mkString(";")}"))

    val domains: Map[String, DataFrame] = buildDomains(domainsSources)
    domains.foreach { case(name, df) => logger.info(s"Domain `${name}`: ${df.schema.mkString(";")}") }

    EtlFeatures.joinDomains(domains, joinRule, checkpointService)
  }

  private def addDtColumn(df: DataFrame): DataFrame = {
    import sql.{functions => sf}
    import implicits._

    df.withColumn(DT_COL_NAME, sf.lit(etlCfg.dt))
  }

  private def addUidTypeColumn(df: DataFrame): DataFrame = {
    import sql.{functions => sf}
    import implicits._

    df.withColumn(UID_TYPE_COL_NAME, sf.lit(etlCfg.uid_type))
  }

  private def findSourceTables(): Seq[DomainSourceDataFrame] = {
    // TODO: use implicits for applying the Reader pattern to config
    sourcesFromConfig(new SourcesFinder(etlCfg)) // read sources info using config view facade
  }

  private def makeDomainSources(domains: Seq[String], sources: Seq[DomainSourceDataFrame]): Seq[DomainSourceDataFrame] = {
    // for each domain: use domain config to build source DF, joined-filtered-projected.
    // - for each source apply source filter-projection spec to table
    // - for each domain (optional: join sources) apply domain filter-projection spec to source

    val sourcesPrepared = for (s <- sources) yield prepareSource(s)
    for (d <- domains) yield makeDomainSource(d, sourcesPrepared.filter(_.domain == d))
  }

  private def prepareSource(source: DomainSourceDataFrame): DomainSourceDataFrame = {
    // find source config, filter and project table according to config
    logger.debug(s"Searching source definition for `${source}` ...")

    val sourceConfig: TableConfig = domainConfig(source.domain).source.tables.find(t =>
      t.name == source.table.name &&
        (t.alias.isEmpty || t.alias.contains(source.source.name))
    ).getOrElse { throw new IllegalArgumentException(s"Malformed config: can't find table definition for `${source}`") }

    logger.debug(s"Found source definition `${sourceConfig}`.\nPreparing source DF ...")
    resultWithLog(
      EtlFeatures.prepareSource(source, sourceConfig),
      logger.debug(s"source ready")
    )
  }

  private def domainConfig(name: String): DomainConfig = {
    logger.debug(s"Searching domain definition for `$name` ...")

    val domainCfg = etlCfg.domains.find(d =>
      d.name == name
    ).getOrElse { throw new IllegalArgumentException(s"Malformed config: can't find domain definition for `$name`") }

    logger.debug(s"Found domain definition `${domainCfg}`")
    domainCfg
  }

  private def makeDomainSource(domain: String, sources: Seq[DomainSourceDataFrame]): DomainSourceDataFrame = {
    //val utypes = sources.map(_.df.get.select(implicits.UID_TYPE_COL_NAME).distinct().collect().mkString(",")).mkString(";")
    //logger.info(s"Building domain `${domain}` source from sources: `${sources}`, uid_types: `${utypes}`  ...")
    logger.debug(s"Building domain `${domain}` source from sources: `${sources}` ...")
    resultWithLog(
      EtlFeatures.makeDomainSource(domainConfig(domain), sources),
      logger.debug("domain source created.")
    )
  }

  private def buildDomains(domains: Seq[DomainSourceDataFrame]): Map[String, DataFrame] = {
    // for each domain: use domain config to collect-features-build-domainDF
    val res = for (d <- domains) yield (d.domain, buildDomain(d.domain, d.df.get))

    res.toMap
  }

  private def buildDomain(name: String, source: DataFrame): DataFrame = {
    // find domain config, build domain
    val cfg = etlCfg.domains.find(_.name == name).getOrElse(sys.error(s"Can't find domain `${name}` in config"))
    logger.debug(s"Building domain `${cfg}` ...`")

    resultWithLog(
      EtlFeatures.buildDomain(cfg, source),
      logger.debug(s"domain `${name}` built.")
    )
  }

  private def doMatching(df: DataFrame): DataFrame = {
    // proceed with matching stage
    // collect input data: records df, cross-table df, uid-types-in, uid-type-out
    // perform matching (explode)
    // call aggregation stage
    import implicits._

    val mc: MatchingConfig = etlCfg.matching.getOrElse(sys.error("Matching table config expected"))
    logger.info(s"Matching config: `${mc}`")

    val inputUidTypes: Seq[String] = mc.uid_types_input
    val outUidType: String = etlCfg.uid_type
    val matchingTableDF: Dataset[MatchingTableRow] = prepareMatchingTable(mc.table, loadMatchingTable(mc.table))

    logger.info(s"Map `${inputUidTypes}` to `${outUidType}` using `${matchingTableDF.schema.mkString(";")}`")
    // TODO: cache function as option: cache, checkpoint, no-op
    val matchedDF: DataFrame = mapUids(df, matchingTableDF, inputUidTypes, outUidType)(df => df.cache())
    logger.info(s"Matching result: ${matchedDF.schema.mkString(";")}")

    logger.info("Aggregating features ...")
    addUidTypeColumn(
      doAggregation(matchedDF.drop(UID_TYPE_COL_NAME))
    )
  }

  private def doAggregation(df: DataFrame): DataFrame = {
    // load domains aggregation config; apply aggregation pipelines
    // df: (uid, domains: *_)
    // aggregation config: map(name -> (pipeline, stages)) for each domain

    val domains: Seq[String] = JoinRule.enumerateItems(partitionJoinRule)
    logger.info(s"Domains to aggregate: `${domains.mkString(",")}`")

    val aggConfig: Map[String, DomainAggregationConfig] = domains.map(name => (name, aggregationConfig(name))).toMap
    logger.info(s"Domains aggregation pipelines: `${aggConfig}`")

    aggregateDomains(df, aggConfig)
  }

  private def aggregationConfig(domain: String): DomainAggregationConfig = {
    // extract domain agg config from job config
    // if no config for domain: alert, use default drop-null-avg config
    // domain_config: Map[String, AggregationConfig]

    val domainCfg = etlCfg.domains.find(d => d.name == domain)
      .getOrElse(sys.error(s"Can't find domain `${domain}` in job config"))

    val cfg: DomainAggregationConfig = domainCfg.agg.getOrElse({
      logger.warn(s"Domain `${domain}` aggregation config not defined, default config will be used. Domain config: `${domainCfg}`")
      defaultAggregationConfig
    })
    logger.info(s"Loaded domain `${domain}` aggregation config `${cfg}`")

    if (cfg.contains(DOMAIN_AGG_KEY)) cfg
    else {
      logger.warn(s"Can't find domain default aggregation section, default will be used." +
        s" Aggregation config for domain must be under `${DOMAIN_AGG_KEY}` key.")

      cfg + (DOMAIN_AGG_KEY -> defaultAggregationConfig(DOMAIN_AGG_KEY))
    }
  }

}

object CmdLineParams {
  import jcommander.Parameter

  @Parameter(names = Array("--etl-config"), required = true, description = "Target config json repr")
  var etl_config: String = _

  @Parameter(names = Array("--log-url"), required = false, description = "Control log endpoint")
  var log_url: String = ""

  @Parameter(names = Array("--min-target-rows"), required = false, description = "Min. acceptable amount of rows in target")
  var min_target_rows: Long = EtlFeatures.MIN_TARGET_ROWS_DEFAULT

  @Parameter(names = Array("--checkpoint-interval"), required = false, description = "Number of joins before checkpoint")
  var checkpoint_interval: Int = EtlFeatures.CHECKPOINT_INTERVAL_DEFAULT

  override def toString: String =
    s"""CmdLineParams(etl_config="$etl_config", log_url="$log_url", min_target_rows=$min_target_rows), checkpoint_interval=${checkpoint_interval}"""

}

case class DomainSourceDataFrame
(
  domain: String,
  source: NameWithAlias,
  table: NameWithAlias,
  df: Option[DataFrame]
)

case class MatchingTableRow(uid1: String, uid2: String, uid1_type: String, uid2_type: String)
