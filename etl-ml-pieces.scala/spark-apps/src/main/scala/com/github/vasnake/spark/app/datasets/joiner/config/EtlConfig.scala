/**
 * Created by vasnake@gmail.com on 2024-07-31
 */
package com.github.vasnake.spark.app.datasets.joiner.config

/** Config for ETL job that creates one output (dt, uid_type) partition
  *
  * @param domains list of feature domains, output columns definitions
  * @param dt output dt partition
  * @param join_rule domains join rule for partition, may be empty if table rule used
  * @param matching optional uid matching stage config
  * @param shuffle_partitions spark option
  * @param write_partitions number of files in hive partition
  * @param table output table name
  * @param table_join_rule domains join rule for table schema, may be empty if only one domain
  * @param uid_type output uid_type
  */
case class EtlConfig(
  table: String,
  dt: String,
  uid_type: String,
  domains: List[DomainConfig],
  join_rule: Option[String],
  matching: Option[MatchingConfig],
  table_join_rule: Option[String],
  shuffle_partitions: Int,
  write_partitions: Option[Int],
) {
  // TODO: add `normalize` method for checking and cleaning empty optional parameters (Some(emptyParam) => None)
  def getWritePartitions: Int = write_partitions.getOrElse(shuffle_partitions)

  override def toString: String =
    s"""EtlConfig(
       |table=$table,
       |dt=$dt,
       |uid_type=$uid_type,
       |domains=List(${domains.mkString(",\n")}),
       |${joinRepr}${tableJoinRepr}${matchingRepr}shuffle_partitions=$shuffle_partitions,
       |write_partitions=$getWritePartitions
       |)""".stripMargin.trim

  private val joinRepr = join_rule match {
    case None => ""
    case Some(v) => if (v.trim.isEmpty) "" else s"join_rule=${v.trim},\n"
  }
  private val tableJoinRepr = table_join_rule match {
    case None => ""
    case Some(v) => if (v.trim.isEmpty) "" else s"table_join_rule=${v.trim},\n"
  }
  private val matchingRepr = matching match {
    case None => ""
    case Some(v) => s"matching=${v},\n"
  }
}

case class MatchingConfig(
  uid_types_input: List[String], // TODO: deprecated, all partitions info should be in table.partitions
  table: TableConfig,
) {
  override def toString: String =
    s"""MatchingConfig(
       |uid_types_input=List(${uid_types_input.mkString(",")}),
       |table=$table
       |)""".stripMargin.trim
}

/** Domain sources definition
  *
  * @param names domain sources names, name could be in form of `foo as bar`
  * @param tables sources tables definitions
  * @param join_rule rule for join sources to one domain source
  */
case class SourceConfig(
  names: List[String],
  tables: List[TableConfig],
  join_rule: Option[String],
) {
  override def toString: String =
    s"""SourceConfig(
       |names=List(${names.mkString(",")}),
       |tables=List(${tables.mkString(",\n")})$joinRuleRepr
       |)""".stripMargin.trim

  private val joinRuleRepr = join_rule match {
    case None => ""
    case Some(v) => s",\njoin_rule=$v"
  }
}

/** Input table definition
  *
  * @param name table name in form of `db.table`
  * @param dt date partition
  * @param partitions partitioning filter
  * @param expected_uid_types required uid_type list, possible three cases:
  *                           None if table don't support uid_type partitioning;
  *                           Some(List.empty) if empty dataset expected (existing uid_types don't required);
  *                           Some(List("FOO", "BAR", "whatnot")) if particular uid_type values are needed from table.
  * @param alias short name for table, used in domains
  * @param features sql.expr statements for features selection
  * @param uid_imitation used in case if table doesn't have (uid, uid_type) partitioning
  * @param where extra filter
  */
case class TableConfig(
  name: String,
  dt: String, // TODO: deprecated parameter, remove it (should be present in `partitions`)
  partitions: List[Map[String, String]],
  expected_uid_types: Option[List[String]],
  alias: Option[String], // TODO: should be mandatory, not optional
  features: Option[List[String]],
  uid_imitation: Option[UidImitationConfig],
  where: Option[String],
) {
  override def toString: String =
    s"""TableConfig(
       |name=$name,
       |dt=$dt,
       |partitions=List(${partitions.mkString(",")})${aliasRepr}${expectedUTypesRepr}${featuresRepr}${uidImitationRepr}${whereRepr}
       |)""".stripMargin.trim

  private val aliasRepr = alias match {
    case None => ""
    case Some(v) => s",\nalias=$v"
  }
  private val expectedUTypesRepr = expected_uid_types match {
    case None => ""
    case Some(v) => s",\nexpected_uid_types=List(${v.mkString(",")})"
  }
  private val featuresRepr = features match {
    case None => ""
    case Some(v) => s",\nfeatures=List(${v.mkString(",")})"
  }
  private val uidImitationRepr = uid_imitation match {
    case None => ""
    case Some(v) => s",\nuid_imitation=${v}"
  }
  private val whereRepr = where match {
    case None => ""
    case Some(v) => s",\nwhere=${v}"
  }
}

/** Data for (uid, uid_type) imitation
  * @param uid column name used as uid
  * @param uid_type literal value for uid_type partition
  */
case class UidImitationConfig(uid: String, uid_type: String) {
  override def toString: String = s"UidImitationConfig(uid=$uid, uid_type=$uid_type)"
}

/** Features domain definition
  *
  * @param name domain name, usually used as output field name
  * @param group_type domain type, MAP_TYPE by default
  * @param cast_type feature type, float by default
  * @param features sql.expr statements for selecting features
  * @param agg features aggregation definitions for each feature and default definition for domain
  * @param source input tables definitions
  */
case class DomainConfig(
  name: String,
  group_type: Option[String],
  cast_type: Option[String],
  features: Option[List[String]],
  agg: Option[Map[String, AggregationConfig]],
  source: SourceConfig,
) {
  override def toString: String =
    s"""DomainConfig(
       |name=$name,
       |${groupTypeRepr}${castTypeRepr}${featuresRepr}${aggRepr}source=$source
       |)""".stripMargin.trim

  private val groupTypeRepr = group_type match {
    case None => ""
    case Some(v) => s"group_type=${v},\n"
  }
  private val castTypeRepr = cast_type match {
    case None => ""
    case Some(v) => s"cast_type=${v},\n"
  }
  private val featuresRepr = features match {
    case None => ""
    case Some(v) => s"features=List(${v.mkString(",")}),\n"
  }
  private val aggRepr = agg match {
    case None => ""
    case Some(v) => s"agg=$v,\n"
  }
}

/** Definition of a transformation stage
  * @param name function name, e.g. `min`, `avg`, `sum`
  * @param kind stage type: `filter`, `agg`, `imputer`, etc
  * @param parameters function parameters, see function spec.
  */
case class AggregationStageConfig(
  name: String,
  kind: String,
  parameters: Map[String, String],
) {
  override def toString: String =
    s"""AggregationStageConfig(
       |name=${name},
       |type=${kind},
       |parameters=${parameters}
       |)""".stripMargin.trim
}

/** Aggregation pipeline config
  *
  * @param pipeline steps enumeration
  * @param stages definitions for each step
  */
case class AggregationConfig(
  pipeline: List[String],
  stages: Map[String, AggregationStageConfig],
) {
  override def toString: String =
    s"""AggregationConfig(
       |pipeline=List(${pipeline.mkString(",")}),
       |stages=${stages}
       |)""".stripMargin.trim
}
