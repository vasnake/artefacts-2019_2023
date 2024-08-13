/** Created by vasnake@gmail.com on 2024-07-23
  */
package org.apache.spark.sql.hive.vasnake

import scala.util._

import com.github.vasnake.spark.io.{ Logging => CustomLogging }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.hive.{ HiveExternalCatalog => HiveExternalCatalog_Orig }
import org.apache.spark.sql.internal.StaticSQLConf._

/** Use custom HiveExternalCatalog to work with metastore in concurrent mode.
  * @param spark session
  * @param poolSize metastore query processors pool size
  */
class MetastoreQueryProcessorWithConnPool(
  spark: SparkSession,
  poolSize: Int,
) extends CustomLogging {
  // convenient pool wrapper
  import MetastoreQueryProcessorWithConnPool._

  require(poolSize > 1, "Pool size must be at least 2")
  require(
    !isDerby(spark),
    "org.apache.derby can't be used in connections pool, set pool size to 1 for using vanilla externalCatalog",
  )
  require(
    isHive(spark),
    "spark.sql.catalogImplementation must be 'hive' for using connections pool, set pool size to 1 for using vanilla externalCatalog",
  )

  preparePool(math.min(METASTORE_CONNECTIONS_POOL_SIZE_MAX, poolSize), spark)
  log.debug(s"Created ${poolActualSize} metastore connections")

  require(poolActualSize > 1, "Pool size must be at least 2")

  def poolActualSize: Int = connections.length

  def closeConnections(): Unit =
    // FYI: calling this on pool.exit make no difference in envoy total_connections monitoring graph
    connections.zipWithIndex.foreach {
      case (conn, idx) =>
        Try {
          HiveExternalCatalog.closeConnection(conn)
        } match {
          case Failure(exception) =>
            log.warn(s"Failed closing connection ${idx + 1}: ${exception.getMessage}", exception)
          case _ => log.debug(s"Connection ${idx + 1} successfully closed.")
        }
    }

  def processQueries[A, B](queries: IndexedSeq[A], oneQueryFun: (A, ExternalCatalog) => Seq[B])
    : IndexedSeq[B] = {
    //  split queries to n lists (n is a number of connections), make a list of tuples (queries, worker),
    //  perform list.par.flatMap, producing result by run(queries).on(connection)

    require(poolActualSize > 1, "Pool size must be at least 2")

    def _runQuery(idx: Int, catalog: ExternalCatalog) = {
      // exec one query on given connection, query could be one of:
      // - catalog.listPartitionsByFilter(database, table, Seq(filter), TIME_ZONE_ID)
      // - catalog.alterPartitions(database, table, parts)
      log.debug(s"Query chunk ${idx + 1} of ${queries.length} ...")
      oneQueryFun(queries(idx), catalog)
    }

    // distribute queries over connections
    val (numQueriesPerConn, reminder) = {
      import scala.math.Integral.Implicits._
      queries.length /% poolActualSize
    }
    log.debug(
      s"Planning to execute total ${queries.length} queries, on ${poolActualSize} connections, with ${numQueriesPerConn} queries per connection, with ${reminder} queries in the last round"
    )

    val res =
      if (numQueriesPerConn == 0)
        // num of queries is less than num of connections
        connections
          .take(reminder)
          .zipWithIndex
          .par // spawn x threads, x = remainder
          .flatMap {
            case (catalog, idx) =>
              withSafeClose(
                // one query on one connection
                fun = _runQuery(idx, catalog),
                close = HiveExternalCatalog.closeConnection(catalog),
              )
          }
      else
        // each connection have at least one query
        connections
          .zipWithIndex
          .par // spawn x threads, x = poolActualSize
          .flatMap {
            case (catalog, catIdx) =>
              withSafeClose(
                // sequence of queries on one connection
                fun = sliceIndices(numQueriesPerConn, reminder, catIdx, queries.length)
                  .flatMap(idx => _runQuery(idx, catalog)),
                close = HiveExternalCatalog.closeConnection(catalog),
              )
          }

    res.toVector
  }

  private def withSafeClose[T](fun: => T, close: => Unit): T = {
    val res = fun

    Try(close) match {
      case Failure(exception) =>
        log.warn(s"Failed closing connection: ${exception.getMessage}", exception)
      case _ => log.debug(s"Connection successfully closed.")
    }

    res
  }
}

object MetastoreQueryProcessorWithConnPool extends CustomLogging {

  // pool constructor, preferable way to create pool
  def apply(spark: SparkSession, poolSize: Int): Option[MetastoreQueryProcessorWithConnPool] = Try(
    new MetastoreQueryProcessorWithConnPool(spark, poolSize)
  ) match {
    case Failure(ex) =>
      log.warn(s"Failed creating connections pool: ${ex.getMessage}")
      None
    case Success(x) =>
      log.info(s"Connections pool(${x.poolActualSize}) successfully created.")
      Some(x)
  }

  val METASTORE_CONNECTIONS_POOL_SIZE_MAX: Int = 32

  private var connections: Seq[ExternalCatalog] = Seq.empty // TODO: consider use cases with concurrent pool clients

  def preparePool(poolSize: Int, spark: SparkSession): Unit = {
    require(
      poolSize <= METASTORE_CONNECTIONS_POOL_SIZE_MAX,
      s"Pool max size cannot be greater than ${METASTORE_CONNECTIONS_POOL_SIZE_MAX}",
    )

    if (connections.length >= poolSize)
      log.debug(s"Pool(${connections.length}) prepared already")
    else {
      val newItemsCount = poolSize - connections.length
      connections =
        connections ++ (1 to newItemsCount).map(_ => HiveExternalCatalog.openConnection(spark))
      log.debug(s"Pool(${connections.length}) prepared")
    }
  }

  def isDerby(spark: SparkSession): Boolean = {
    val hec = spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog_Orig]

    val settings = Seq("javax.jdo.option.ConnectionURL", "javax.jdo.option.ConnectionDriverName")
      .map(name => name -> hec.client.getConf(name, "undefined"))

    settings.foreach { case (name, value) => log.info(s"$name: `$value`") }

    settings.map(_._2).exists(_.contains("derby"))
  }

  def isHive(spark: SparkSession): Boolean =
    spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => true
      case _ => false
    }

  def sliceIndices(
    numItems: Int,
    reminder: Int,
    sliceIdx: Int,
    totalItems: Int,
  ): Seq[Int] = {
    assert(
      numItems > 0 && reminder >= 0 && totalItems >= numItems && sliceIdx >= 0 && (sliceIdx * numItems + reminder) < totalItems,
      "Inconsistent sliceIndices parameters",
    )

    val start = sliceIdx * numItems
    val end = start + numItems
    val indices = start until end

    val tailIdx = totalItems - (sliceIdx + 1)

    if (totalItems - tailIdx <= reminder) indices :+ tailIdx
    else indices
  }
}
