/** Created by vasnake@gmail.com on 2024-08-08
  */
package com.github.vasnake.spark.app.datasets

/*
 Adhoc datasets comparison: prod vs dev tables, e.g. banner_feature.
 Heavily underdeveloped, WIP.

<pre>

  # Check table structure

 hive> SHOW CREATE TABLE prod.banner_feature;

 LOCATION
  'hdfs://hacluster/data/hive/prod.db/banner_feature'

 CREATE TABLE `prod.banner_feature`(
  `uid` string COMMENT 'Banner id',
  `event_cats_score` map<string,float> COMMENT 'Log-normalized event category counts',
  `platform` map<string,int> COMMENT 'Platform category from package associated with banner',
...
  `banner_age` array<int> COMMENT 'Amount of days from create and update')
COMMENT 'Banner features'
PARTITIONED BY (
  `dt` string COMMENT 'Partition date',
  `uid_type` string COMMENT 'Type of uid, e.g. BANNER')

 hive> SHOW CREATE TABLE dev.banner_feature;
...

 # Check files: existence, size

 hdfs_list_r /.../banner_feature/dt=2022-12-01/uid_type=BANNER/
-rw-rw-rw-+  3 rbauth rbauth     18.8 M 2022-12-02 13:03 /.../banner_feature/dt=2022-12-01/uid_type=BANNER/part-00000-5e3d6b20-de3a-47c4-8915-ecaee252bf93.c000

</pre>

  Setup runtime
<pre>

# 100 gb * 2 datasets * 4 parts = 800; 800 tasks * 2 safety * 2 compress = 3200
export SSSP=4096

# https://yarn.hadoop/cluster/scheduler?openQueues=root.dev
export QNAME=root.dev.foo.priority

truncate --size 0 ~/spark_driver.log; \
spark-shell --verbose --master yarn --name "$TICKET-test" \
  --queue ${QNAME} \
  --num-executors 2 --executor-cores 4 --executor-memory 16G --driver-memory 4G \
  --conf spark.executor.memoryOverhead=8G \
  --conf spark.driver.maxResultSize=2G \
  --conf spark.sql.shuffle.partitions=${SSSP} \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.maxExecutors=256 \
  --conf spark.dynamicAllocation.minExecutors=2 \
  --conf spark.speculation=true \
  --conf spark.kryoserializer.buffer.max=1024m \
  --conf spark.hadoop.dfs.replication=1 \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/home/$USER/driver_log4j.properties"

</pre>

 */
object CompareDatasetsApp {
  /*
   * Code for copy-paste and execute in spark-shell
   */
  object CopyPasteToSparkShell {
    import org.apache.spark.sql
    import org.apache.spark.sql.{ DataFrame, SparkSession, Row, functions => sqlfn }
    import scala.collection.mutable

    type MapCol = Map[String, Float]
    type ArrayCol = mutable.WrappedArray[Float]

    object Functions {
      def loadHivePartition(
        db: String,
        table: String,
        dt: String,
        uid_type: String,
      )(implicit
        spark: SparkSession
      ): DataFrame = {
        import sql.functions.lit
        import spark.implicits._

        println(s"""
           |Loading partition:
           |`${db}.${table}/dt=$dt/uid_type=$uid_type`
           | ... """.stripMargin)

        spark
          .read
          .table(s"${db}.${table}")
          .where($"dt" === lit(dt))
          .where($"uid_type" === lit(uid_type))
          .drop("dt", "uid_type")
      }

      def comparePartitions(
        got: DataFrame,
        expected: DataFrame,
        ops: DataFrameActions,
      ): DataFrame = {
        // TODO: add some config. At least aliases: actualDF alias (dev), expectedDF alias (prod); pass it to each stage of pipeline
        implicit val ss: SparkSession = got.sparkSession
        val gotDF = ops.cache(got)
        val expectedDF = ops.cache(expected)

        println("Checking schema ...")
        val gotFields = gotDF.schema.fields
        val expectedFields = expectedDF.schema.fields
        require(
          gotFields.length == expectedFields.length,
          s"got fields count must be == expected feilds count, ${gotFields.length} == ${expectedFields.length}",
        )
        val schemaEqual = gotFields.zip(expectedFields).forall {
          case (gf, ef) =>
            gf.name == ef.name && gf.dataType == ef.dataType && gf.nullable == ef.nullable
        }
        require(
          schemaEqual,
          s"1) got fields set must be == expected fields set,\n`${gotDF.schema}`\n!=\n`${expectedDF.schema}`",
        )
        require(
          gotDF.schema.toString() == expectedDF.schema.toString(),
          s"2) got fields set must be == expected fields set,\n`${gotDF.schema}`\n!=\n`${expectedDF.schema}`",
        )
        println(s"Schema OK,\n`${gotDF.schema}`\n==\n`${expectedDF.schema}`")

        println("Checking rows count ...")
        val gotCount = gotDF.count()
        val expectedCount = expectedDF.count()

        // require(
        //  gotCount == expectedCount,
        //  s"actual count must be == expected count, ${gotCount} != ${expectedCount}"
        // )
        println(s"actual count must be == expected count, ${gotCount} ?= ${expectedCount}")

        println("Checking uid set (inner join) ...")
        val joined = {
          val joined = ops.cache(ops.checkpoint(packAndJoin(expectedDF, gotDF)))
          println("Checking joined rows count ...")
          val joinedCount = joined.count()

          // require(
          //  joinedCount == expectedCount,
          //  s"joined count must be == expected, ${joinedCount} != ${expectedCount}"
          // )
          println(s"joined count must be == expected, ${joinedCount} ?= ${expectedCount}")

          joined
        }

        joined.show(truncate = false)

        println("Calculating columns diff ...")

        computeJoinedDiff(
          joined,
          DiffConfig(
            // columns = expectedFields.filter(_.name != "uid").map(_.name),
            columns = joined.selectExpr("prod.*").schema.fieldNames
          ),
        )
      }

      def packColumnsToStruct(
        df: DataFrame,
        structAlias: String,
        keyColName: String = "uid",
      ): DataFrame =
        df.select(
          df(keyColName),
          sqlfn
            .struct(
              df.columns.filter(colname => colname != keyColName).map(sqlfn.col): _*
            )
            .alias(structAlias),
        )

      def packAndJoin(
        left: DataFrame,
        right: DataFrame,
        leftName: String = "prod",
        rightName: String = "dev",
        key: Seq[String] = Seq("uid"),
        joinType: String = "inner",
      ): DataFrame =
        packColumnsToStruct(left, leftName)
          .join(
            packColumnsToStruct(right, rightName),
            key,
            joinType,
          )

      def computeJoinedDiff(df: DataFrame, diffConfig: DiffConfig): DataFrame = {
        implicit val spark: SparkSession = df.sparkSession

        import scala.math.{ max, abs }
        import org.apache.spark.sql.catalyst.encoders.RowEncoder
//      import org.apache.spark.broadcast.Broadcast
//      import spark.implicits._

        // spark shell failed on broadcast
        // val config: Broadcast[DiffConfig] = spark.sparkContext.broadcast(
        //  diffConfig.copy(
        //    leftStructIndex = df.schema.fieldIndex(diffConfig.leftStructName),
        //    rightStructIndex = df.schema.fieldIndex(diffConfig.rightStructName)
        //  )
        // )

        val columns: Array[String] = diffConfig.columns
        val leftStructName: String = diffConfig.leftStructName
        val rightStructName: String = diffConfig.rightStructName
        val leftStructIndex = df.schema.fieldIndex(diffConfig.leftStructName)
        val rightStructIndex = df.schema.fieldIndex(diffConfig.rightStructName)
        println(s"Find diff for columns: ${columns.mkString("\n")}")

        val withDiff = df.mapPartitions { rows =>
          // partition processing stack, save some values here
          // val cfg = config.value

          val domainIndex: Map[String, Int] = columns.zipWithIndex.toMap

          // spark shell serialization problem, again. inline function by-hand:
          @inline
          def compareDouble(
            a: Double,
            b: Double,
            epsilon: Double = 0.00001,
          ): Int =
            if (a == b) 0
            else if (a.isNaN && b.isNaN) 0
            else if (a.isNaN) 1
            else if (b.isNaN) -1
            else if (a.isPosInfinity) 1
            else if (b.isPosInfinity) -1
            else if (a.isNegInfinity) -1
            else if (b.isNegInfinity) 1
            else {
              // two valid numbers
              val maxV = max(1.0, max(abs(a), abs(b)))
              val diff = a - b

              if (abs(diff) <= epsilon * maxV) 0
              else if (diff < 0) -1
              else 1
            }

          val cmpInt: (Int, Int) => Int =
            (a: Int, b: Int) => a.compareTo(b)

          val cmpFloat: (Float, Float) => Int =
            (a: Float, b: Float) => compareDouble(a, b, epsilon = 0.00001)

          def genericDiff(
            left: Any,
            right: Any,
            dataType: sql.types.DataType,
            leftName: String = "prod",
            rightName: String = "dev",
          ): String = {
            import sql.types._
            import scala.collection.mutable
            // val delta: Double = 0.00009

            def arraysDiff[T](
              left: mutable.WrappedArray[Any],
              right: mutable.WrappedArray[Any],
              cmp: (T, T) => Int,
            ): String =
              if (left.length == right.length) {
                val diff: Seq[String] = (0 until left.length).foldLeft(Seq.empty[String]) {
                  (acc, i) =>
                    if (left(i) != null && right(i) != null) {
                      val d = cmp(left(i).asInstanceOf[T], right(i).asInstanceOf[T])
                      if (d < 0) acc :+ s"${leftName} < ${rightName} idx ${i}"
                      else if (d > 0) acc :+ s"${leftName} > ${rightName} idx ${i}"
                      else acc
                    }
                    else if (left(i) == null && right(i) == null) acc
                    else
                      acc :+ {
                        if (left(i) == null) s"${leftName} is null idx ${i}"
                        else s"${rightName} is null idx ${i}"
                      }
                }

                diff.mkString(", ")
              }
              else s"arrays size different, ${left.length} != ${right.length}"

            def mapsDiff[T](
              left: Map[String, Any],
              right: Map[String, Any],
              cmp: (T, T) => Int,
            ): String =
              if (left.size == right.size) {
                val diff: Seq[String] =
                  left.keys.foldLeft(Seq.empty[String]) { (acc, key) =>
                    right.get(key) match {
                      case None => acc :+ s"no key `${key}` in ${rightName}"
                      case Some(rv) =>
                        val lv =
                          left.getOrElse(key, sys.error(s"No value in ${leftName}, impossible!"))
                        // TODO: extract compare method
                        if (lv != null && rv != null) {
                          val d = cmp(lv.asInstanceOf[T], rv.asInstanceOf[T])
                          if (d < 0) acc :+ s"${leftName} < ${rightName} key ${key}"
                          else if (d > 0) acc :+ s"${leftName} > ${rightName} key ${key}"
                          else acc
                        }
                        else if (lv == null && rv == null) acc
                        else
                          acc :+ {
                            if (lv == null) s"${leftName} is null key ${key}"
                            else s"${rightName} is null key ${key}"
                          }
                    }
                  }
                diff.mkString(", ")
              }
              else s"maps size different, ${left.size} != ${right.size}"

            def toArray(a: Any) = a.asInstanceOf[mutable.WrappedArray[Any]]
            def toMap(m: Any) = m.asInstanceOf[Map[String, Any]]

            dataType match {
              case ArrayType(DataTypes.IntegerType, _) =>
                arraysDiff[Int](toArray(left), toArray(right), cmpInt)
              case ArrayType(DataTypes.FloatType, _) =>
                arraysDiff[Float](toArray(left), toArray(right), cmpFloat)
              case MapType(DataTypes.StringType, DataTypes.IntegerType, _) =>
                mapsDiff[Int](toMap(left), toMap(right), cmpInt)
              case MapType(DataTypes.StringType, DataTypes.FloatType, _) =>
                mapsDiff[Float](toMap(left), toMap(right), cmpFloat)
              case _ => sys.error(s"Unknown type ${dataType}")
            }
          }

          def findDiff(
            domainName: String,
            row: sql.Row,
          ): String = {

            // yes, I'm sure it's a struct
            val leftStruct = row(leftStructIndex).asInstanceOf[sql.Row]
            val rightStruct = row(rightStructIndex).asInstanceOf[sql.Row]

            val colIdx = domainIndex(domainName)

            if (!leftStruct.isNullAt(colIdx) && !rightStruct.isNullAt(colIdx))
              // println("Compare not null values")
              genericDiff(
                leftStruct(colIdx),
                rightStruct(colIdx),
                leftStruct.schema(colIdx).dataType,
                leftStructName,
                rightStructName,
              )
            else if (leftStruct.isNullAt(colIdx) && rightStruct.isNullAt(colIdx))
              // println("Domains are equal, both `is null`")
              ""
            else
            // println("Domains are different, one of them is null")
            if (leftStruct.isNullAt(colIdx)) s"${leftStructName} is null"
            else s"${rightStructName} is null"
          }

          rows.map { row =>
            val diff: List[String] = columns.foldLeft(List.empty[String]) { (acc, col) =>
              val col_diff: String = findDiff(col, row)
              if (col_diff.isEmpty) acc
              else acc :+ s"${col}: ${col_diff}"
            }

            val appendix =
              if (diff.isEmpty) Seq(true, "")
              else Seq(false, diff.mkString("\n"))

            Row.fromSeq(row.toSeq ++ appendix)
          }
        }(
          RowEncoder(
            df.schema
              .add(sql.types.StructField("is_equal", sql.types.DataTypes.BooleanType))
              .add(sql.types.StructField("diff", sql.types.DataTypes.StringType))
          )
        )

        withDiff.where("not is_equal")
      }

      def checkpoint(df: DataFrame, path: String): DataFrame = {
        println(s"Checkpoint to `${path}` ...")
        df.write.mode("overwrite").option("compression", "gzip").parquet(path)
        df.sparkSession.read.parquet(path)
      }
    }

    trait DataFrameActions {
      def cache(df: DataFrame): DataFrame
      def checkpoint(df: DataFrame): DataFrame
    }

    case class DiffConfig(
      columns: Array[String],
      leftStructName: String = "prod",
      leftStructIndex: Int = -1,
      rightStructName: String = "dev",
      rightStructIndex: Int = -1,
    )
  }

  object InteractiveSparkShellSession {
    import org.apache.spark.sql
    import org.apache.spark.sql.{ DataFrame, SparkSession }
    import org.apache.spark.storage.StorageLevel
//    import scala.collection.mutable

    val funcs: CopyPasteToSparkShell.Functions.type = CopyPasteToSparkShell.Functions
    import CopyPasteToSparkShell._

    @transient
    implicit val spark: SparkSession = sql
      .SparkSession
      .builder()
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    val hdfsBasePath: String = "hdfs:/.../dev-prod-compare"

    case class Actions(hdfsSubdir: String) extends DataFrameActions {
      private var checkpointCount = 0
      override def cache(df: DataFrame): DataFrame = df.persist(StorageLevel.MEMORY_AND_DISK)
      override def checkpoint(df: DataFrame): DataFrame = {
        checkpointCount += 1
        funcs.checkpoint(df, s"${hdfsBasePath}/${hdfsSubdir}/checkpoint/${checkpointCount}")
      }
    }

    def checkHidDataset30(
      _dt: String = "2023-01-08"
    )(implicit
      spark: SparkSession
    ): DataFrame = {
      val (prodDb, prodTable) = ("prod", "hid_dataset_3_0")
      val (devDb, devTable) = ("dev", "hid_dataset_3_0")
      val (dt, uid_type) = (_dt, "HID")

      val prodPartition = funcs.loadHivePartition(prodDb, prodTable, dt, uid_type)
      val devPartition = funcs.loadHivePartition(devDb, devTable, dt, uid_type)

      val df = funcs.comparePartitions(devPartition, prodPartition, Actions(prodTable))

      funcs
        .checkpoint(df, s"${hdfsBasePath}/${prodTable}/diff")
        .persist(StorageLevel.MEMORY_AND_DISK_2)
    }

    val diff: DataFrame = checkHidDataset30("2023-03-02")
    diff.count()
    // Manual: check diff content, do some analysis, eye-ball search, etc.
    // val diff = spark.read.parquet("hdfs:/.../dev-prod-compare/hid_dataset_3_0/diff")
    // val diff2 = diff.where("diff not like '%topics100:%' and diff not like '%all_profs:%'")
  }
}
