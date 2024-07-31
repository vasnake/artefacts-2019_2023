/** Created by vasnake@gmail.com on 2024-07-31
 */
package com.github.vasnake.spark.app.datasets.joiner.config

import com.github.vasnake.core.text.StringToolbox

class SourcesFinder(cfg: EtlConfig) extends ISourcesConfigView {
  override def domains: Seq[String] = for {
    domain <- cfg.domains
  } yield domain.name

  override def sources(domain: String): Seq[NameWithAlias] = {
    import StringToolbox._
    implicit val sep: Separators = Separators(" as ")

    val sourceNames = cfg.domains.filter(d => d.name == domain).flatMap(d => d.source.names)

    sourceNames map { nameExpr =>
      nameExpr.splitTrim.toSeq match {
        case Seq(name, alias) => NameWithAlias(name, alias)
        case Seq(name) => NameWithAlias(name, name)
        case _ =>
          val msg =
            s"Invalid config: domain `${domain}` have malformed source name `${nameExpr}` in source.names `${sourceNames}`"
          throw new IllegalArgumentException(msg)
      }
    }
  }

  override def table(source: NameWithAlias): NameWithAlias = {
    val tables = for {
      domain <- cfg.domains
      table <- domain.source.tables if table.alias.contains(source.name) // TODO: alias should be mandatory, not Option
    } yield NameWithAlias(table.name, table.alias.getOrElse(table.name))

    require(tables.nonEmpty, s"Invalid config: can't find table for source name ${source}")

    tables.head
  }
}

trait ISourcesConfigView {
  def domains: Seq[String]
  def sources(domain: String): Seq[NameWithAlias]
  def table(source: NameWithAlias): NameWithAlias // retrieve table from list of tables using source name as index
}

case class NameWithAlias(name: String, alias: String)

case class MatchingTableRow(
  uid1: String,
  uid2: String,
  uid1_type: String,
  uid2_type: String,
)
