/**
 * Created by vasnake@gmail.com on 2024-08-14
 */
package com.github.vasnake.spark.app.datasets

import com.github.vasnake.spark.test._
import com.github.vasnake.core.text.StringToolbox
import com.github.vasnake.spark.dataset.transform.Joiner.JoinRule
import com.github.vasnake.spark.test.DataFrameHelpers

import org.apache.spark.sql
import sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.util.Try

import org.scalatest.flatspec._
// import org.scalatest.matchers._

class JoinerAppTest extends AnyFlatSpec with DataFrameHelpers  with SimpleLocalSparkWithHive {

  import spark.implicits._
  import EtlFeaturesFunctionsTest._
  import StringToolbox._
  import DefaultSeparators._
  import joiner.config._
  import joiner._
  import joiner.implicits._
  import sql.{functions => sf}
  import com.github.vasnake.spark.dataset.transform.Joiner.parseJoinRule

  override def beforeAll(): Unit = {
    super.beforeAll()
    // TODO: create catalyst UDF
    // https://github.com/jeromebanks/brickhouse/blob/4292ca32001bca5bcbca29a7150a42ca436e2864/src/main/resources/brickhouse.hql#L16
    val alias = "brickhouse_combine"
    val classPath = "brickhouse.udf.collect.CombineUDF"
    val expr = s"CREATE FUNCTION IF NOT EXISTS ${alias} AS '${classPath}'"
    // val expr = s"CREATE OR REPLACE TEMPORARY FUNCTION ${alias} AS '${classPath}'"
    spark.sql(expr).collect()
  }

  it should "build array domain from primitive cols" in {
    val df = Seq(SourceRow("uid: 42, uid_type: OKID, pFeature1: true, pFeature2: 2, pFeature3: 3")).toDF
      .selectCSVcols("uid,uid_type,pFeature1 as f1,pFeature2 as f2,pFeature3 as f3,pFeature4 as f4")

    val res = EtlFeatures.buildArrayDomain("foo", df, "float")
    val expected = Seq("[42,OKID,WrappedArray(1.0, 2.0, 3.0, null)]")
    val actual = res.collect.map(_.toString)

    res.columns should contain theSameElementsInOrderAs "uid,uid_type,foo".splitTrim
    actual should contain theSameElementsAs expected
  }

  it should "build array domain from array cols" in {
    val df = Seq(SourceRow("uid: 42, uid_type: OKID, aFeature: 1;2;3")).toDF
      .selectCSVcols("uid,uid_type,aFeature as f1,aFeature as f2")

    val res = EtlFeatures.buildArrayDomain("bar", df, "float")
    val expected = Seq("[42,OKID,WrappedArray(1.0, 2.0, 3.0, 1.0, 2.0, 3.0)]")
    val actual = res.collect.map(_.toString)

    res.columns should contain theSameElementsInOrderAs "uid,uid_type,bar".splitTrim
    actual should contain theSameElementsAs expected
  }

  it should "build null array domain from map cols" in {
    val df = Seq(SourceRow("uid: 42, uid_type: OKID, mFeature: a=1;b=2;c=3")).toDF
      .selectCSVcols("uid,uid_type,mFeature as f1,mFeature as f2")

    val res = EtlFeatures.buildArrayDomain("baz", df, "float")
    val expected = Seq("[42,OKID,null]")
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,IntegerType,true);" +
      "StructField(uid_type,StringType,true);" +
      "StructField(baz,ArrayType(FloatType,true),true)"
    )

    actual should contain theSameElementsAs expected
  }

  it should "build array domain from mixed cols" in {
    val df = Seq(SourceRow("uid: 42, uid_type: OKID, mFeature: a=1;b=2;c=3, aFeature: 9;8;7, pFeature2: 2, pFeature3: 3")).toDF
      .selectCSVcols("uid,uid_type,mFeature as f1,aFeature as f2,pFeature1 as f3,pFeature2 as f4,pFeature3 as f5")

    val res = EtlFeatures.buildArrayDomain("quix", df, "float")
    val expected = Seq("[42,OKID,WrappedArray(null, 2.0, 3.0, 9.0, 8.0, 7.0)]")
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,IntegerType,true);" +
        "StructField(uid_type,StringType,true);" +
        "StructField(quix,ArrayType(FloatType,true),true)"
    )
    actual should contain theSameElementsAs expected
  }

  it should "build array domain if merging array is null" in {
    val df = {
      val df1 = inputDF(spark).withColumn("uid", sf.lit("42")).withColumn("uid_type", sf.lit("OKID"))
      val df2 = df1.withColumn("d_array_nullfield", sf.expr("array(11, 12)"))
        .withColumn("f_array_nullfield", sf.expr("array(21, 22, 23)"))
        .withColumn("uid", sf.lit("41"))

      df1.union(df2)
    }

    val res = EtlFeatures.buildArrayDomain("quuz", df, "float")
    val expected = Seq(
      "[42,OKID,WrappedArray(342.0, null, null, null, 442.0, null, null, null, null)]",
      "[41,OKID,WrappedArray(342.0, null, 11.0, 12.0, 442.0, null, 21.0, 22.0, 23.0)]"
    )
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,StringType,false);" +
        "StructField(uid_type,StringType,false);" +
        "StructField(quuz,ArrayType(FloatType,true),true)"
    )
    actual should contain theSameElementsAs expected
  }

  it should "build array domain from mixed cols with various types" in {
    val df = inputDF(spark)
      .drop("d_array_nullfield", "f_array_nullfield")
      .withColumn("uid", sf.lit("42")).withColumn("uid_type", sf.lit("OKID"))

    val res = EtlFeatures.buildArrayDomain("corge", df, "float")
    val expected = Seq("[42,OKID,WrappedArray(342.0, null, 442.0, null)]")
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,StringType,false);" +
        "StructField(uid_type,StringType,false);" +
        "StructField(corge,ArrayType(FloatType,true),true)"
    )
    actual should contain theSameElementsAs expected
  }

  it should "fail building array domain from null cols" in {
    // non-empty df but null array columns can't be used, array size can't be computed, process should fail
    val df = inputDF(spark).withColumn("uid", sf.lit("42")).withColumn("uid_type", sf.lit("OKID"))
    val err = intercept[IllegalArgumentException] {
      EtlFeatures.buildArrayDomain("grault", df, "float")
    }
    assert(err.getMessage === """requirement failed: ARRAY_TYPE domain parts must have size >= 0. Domain `grault`, parts sizes `Map(d_array_values -> 2, d_array_nullfield -> -1, f_array_values -> 2, f_array_nullfield -> -1)`""")
  }

  it should "not fail building array domain from empty source" in {
    // empty source should not fail but generate empty domain
    val df = inputDF(spark).withColumn("uid", sf.lit("42")).withColumn("uid_type", sf.lit("OKID")).where("uid = 24")
    val res = EtlFeatures.buildArrayDomain("garply", df, "float")
    assert(res.schema.mkString(";") ===
      "StructField(uid,StringType,false);" +
        "StructField(uid_type,StringType,false);" +
        "StructField(garply,ArrayType(FloatType,true),true)"
    )
    assert(res.count() === 0)
  }

  it should "build null array domain from collection where all elems are null" in {
    val df = Seq(SourceRow("uid: 42, uid_type: OKID, mFeature: a=1;b=2;c=3, aFeature: ;;")).toDF
      .selectCSVcols("uid,uid_type,mFeature as f1,aFeature as f2,pFeature1 as f3,pFeature2 as f4,pFeature3 as f5")

    val res = EtlFeatures.buildArrayDomain("baz", df, "float")
    val expected = Seq("[42,OKID,null]")
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,IntegerType,true);" +
        "StructField(uid_type,StringType,true);" +
        "StructField(baz,ArrayType(FloatType,true),true)"
    )

    actual should contain theSameElementsAs expected
  }

  it should "build null array domain from empty collection" in {
    val df = Seq(SourceRow("uid: 42, uid_type: OKID, mFeature: a=1;b=2;c=3")).toDF
      .selectCSVcols("uid,uid_type,mFeature as f1,array() as f2")

    val res = EtlFeatures.buildArrayDomain("baz", df, "float")
    val expected = Seq("[42,OKID,null]")
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,IntegerType,true);" +
        "StructField(uid_type,StringType,true);" +
        "StructField(baz,ArrayType(FloatType,true),true)"
    )

    actual should contain theSameElementsAs expected
  }

  it should "parse base64 encoded config" in {
    import DefaultB64Mapping.extraCharsMapping

    val b64Encoded = Seq(
      (1, """eyJkb21haW5zIjpbeyJjYXN0X3R5cGUiOiJmbG9hdCIsImdyb3VwX3R5cGUiOiJNQVBfVFlQRSIsImluX3BhcnRpdGlvbl9qb2luX3J1bGUiOnRydWUsIm5hbWUiOiJ0b3BpY3MiLCJzb3VyY2UiOnsibmFtZXMiOlsidG9waWNzX3NvdXJjZSJdLCJ0YWJsZXMiOlt7ImFsaWFzIjoidG9waWNzX3NvdXJjZSIsImR0IjoiMjAyMC0wNC0yMiIsImV4cGVjdGVkX3VpZF90eXBlcyI6WyJHQUlEIl0sIm5hbWUiOiJ0ZXN0X2RiLnRvcGljcyIsInBhcnRpdGlvbnMiOlt7ImR0IjoiMjAyMC0wNC0yMiIsInVpZF90eXBlIjoiR0FJRCJ9XX1dfX1dLCJkdCI6IjIwMjAtMDQtMjIiLCJqb2luX3J1bGUiOiIiLCJzaHVmZmxlX3BhcnRpdGlvbnMiOjUwMCwidGFibGUiOiJ0ZXN0X2RiLnRhcmdldF90YWJsZSIsInRhYmxlX2pvaW5fcnVsZSI6IiIsInVpZF90eXBlIjoiR0FJRCJ9"""),
      (3, """eyJkb21haW5zIjpbeyJhZ2ciOnsiZG9tYWluIjp7InBpcGVsaW5lIjpbImZpbHRlciIsImFnZyJdLCJzdGFnZXMiOnsiYWdnIjp7ImtpbmQiOiJhZ2ciLCJuYW1lIjoiYXZnIiwicGFyYW1ldGVycyI6eyJkZWZhdWx0IjoibnVsbCJ9fSwiZmlsdGVyIjp7ImtpbmQiOiJmaWx0ZXIiLCJuYW1lIjoibmEiLCJwYXJhbWV0ZXJzIjp7ImRyb3AiOiJuYW4sIG51bGwifX19fSwiZml4ZWRfcHJpY2UiOnsicGlwZWxpbmUiOlsiYWdnIl0sInN0YWdlcyI6eyJhZ2ciOnsia2luZCI6ImFnZyIsIm5hbWUiOiJhdmciLCJwYXJhbWV0ZXJzIjp7ImRlZmF1bHQiOiJuYW4ifX19fX0sImNhc3RfdHlwZSI6ImZsb2F0IiwiZmVhdHVyZXMiOlsidG9waWNzM19zb3VyY2UuKiIsImIuKiIsImEucHJpY2UgYXMgZml4ZWRfcHJpY2UiXSwiZ3JvdXBfdHlwZSI6Ik1BUF9UWVBFIiwiaW5fcGFydGl0aW9uX2pvaW5fcnVsZSI6dHJ1ZSwibmFtZSI6InRvcGljcyIsInNvdXJjZSI6eyJqb2luX3J1bGUiOiJhIGZ1bGxfb3V0ZXIgKGIgbGVmdF9vdXRlciB0b3BpY3MzX3NvdXJjZSkiLCJuYW1lcyI6WyJ0b3BpY3MxX3NvdXJjZSBhcyBhIiwidG9waWNzMl9zb3VyY2UgYXMgYiIsInRvcGljczNfc291cmNlIl0sInRhYmxlcyI6W3siYWxpYXMiOiJ0b3BpY3MxX3NvdXJjZSIsImR0IjoiMjAyMC0wMy0yMSIsImV4cGVjdGVkX3VpZF90eXBlcyI6WyJFTUFJTCJdLCJmZWF0dXJlcyI6WyJmZWF0dXJlc1sncHJpY2UnXSBhcyBwcmljZSIsImZlYXR1cmVzIl0sIm5hbWUiOiJ0ZXN0X2RiLnRvcGljc19vbmUiLCJwYXJ0aXRpb25zIjpbeyJkdCI6IjIwMjAtMDMtMjEiLCJyZWdpb24iOiI3OTkifV0sInVpZF9pbWl0YXRpb24iOnsidWlkIjoiaWQiLCJ1aWRfdHlwZSI6IkVNQUlMIn0sIndoZXJlIjoiYWdlID4gMjEifSx7ImFsaWFzIjoidG9waWNzMl9zb3VyY2UiLCJkdCI6IjIwMjAtMDMtMjgiLCJleHBlY3RlZF91aWRfdHlwZXMiOltdLCJuYW1lIjoidGVzdF9kYi50b3BpY3NfdHdvIiwicGFydGl0aW9ucyI6W3siZHQiOiIyMDIwLTAzLTI4In1dfSx7ImFsaWFzIjoidG9waWNzM19zb3VyY2UiLCJkdCI6IjIwMjAtMDMtMjgiLCJleHBlY3RlZF91aWRfdHlwZXMiOltdLCJuYW1lIjoidGVzdF9kYi50b3BpY3NfdGhyZWUiLCJwYXJ0aXRpb25zIjpbeyJkdCI6IjIwMjAtMDMtMjgifV19XX19LHsiYWdnIjp7ImRvbWFpbiI6eyJwaXBlbGluZSI6WyJhZ2ciXSwic3RhZ2VzIjp7ImFnZyI6eyJraW5kIjoiYWdnIiwibmFtZSI6ImF2ZyIsInBhcmFtZXRlcnMiOnsiZGVmYXVsdCI6Im51bGwifX19fX0sImNhc3RfdHlwZSI6ImRvdWJsZSIsImdyb3VwX3R5cGUiOiJBUlJBWV9UWVBFIiwiaW5fcGFydGl0aW9uX2pvaW5fcnVsZSI6dHJ1ZSwibmFtZSI6InByb2ZzIiwic291cmNlIjp7Im5hbWVzIjpbInByb2ZzX3NvdXJjZSJdLCJ0YWJsZXMiOlt7ImFsaWFzIjoicHJvZnNfc291cmNlIiwiZHQiOiIyMDIwLTAzLTI4IiwiZXhwZWN0ZWRfdWlkX3R5cGVzIjpbIkVNQUlMIl0sIm5hbWUiOiJ0ZXN0X2RiLnByb2ZzIiwicGFydGl0aW9ucyI6W3siZHQiOiIyMDIwLTAzLTI4IiwidWlkX3R5cGUiOiJFTUFJTCJ9XX1dfX0seyJhZ2ciOnsiZG9tYWluIjp7InBpcGVsaW5lIjpbImFnZyJdLCJzdGFnZXMiOnsiYWdnIjp7ImtpbmQiOiJhZ2ciLCJuYW1lIjoiYXZnIiwicGFyYW1ldGVycyI6eyJkZWZhdWx0IjoibnVsbCJ9fX19fSwiY2FzdF90eXBlIjoiZmxvYXQiLCJncm91cF90eXBlIjoiUFJFRklYX1RZUEUiLCJpbl9wYXJ0aXRpb25fam9pbl9ydWxlIjpmYWxzZSwibmFtZSI6Imdyb3VwcyIsInNvdXJjZSI6eyJuYW1lcyI6WyJncm91cHNfc291cmNlIl0sInRhYmxlcyI6W3siYWxpYXMiOiJncm91cHNfc291cmNlIiwiZHQiOiIyMDIwLTAzLTI4IiwiZXhwZWN0ZWRfdWlkX3R5cGVzIjpbXSwibmFtZSI6InRlc3RfZGIuZ3JvdXBzIiwicGFydGl0aW9ucyI6W3siZHQiOiIyMDIwLTAzLTI4In1dfV19fV0sImR0IjoiMjAyMC0wMy0yOCIsImpvaW5fcnVsZSI6InByb2ZzIGxlZnRfb3V0ZXIgdG9waWNzIiwic2h1ZmZsZV9wYXJ0aXRpb25zIjo1MDAsInRhYmxlIjoidGVzdF9kYi50YXJnZXRfdGFibGUiLCJ0YWJsZV9qb2luX3J1bGUiOiJ0b3BpY3MgZnVsbF9vdXRlciAocHJvZnMgbGVmdF9vdXRlciBncm91cHMpIiwidWlkX3R5cGUiOiJFTUFJTCJ9"""),
      (3, """eyJkb21haW5zIjpbeyJhZ2ciOnsiZG9tYWluIjp7InBpcGVsaW5lIjpbImZpbHRlciIsImFnZyJdLCJzdGFnZXMiOnsiYWdnIjp7ImtpbmQiOiJhZ2ciLCJuYW1lIjoiYXZnIiwicGFyYW1ldGVycyI6eyJkZWZhdWx0IjoibnVsbCJ9fSwiZmlsdGVyIjp7ImtpbmQiOiJmaWx0ZXIiLCJuYW1lIjoibmEiLCJwYXJhbWV0ZXJzIjp7ImRyb3AiOiJuYW4sIG51bGwifX19fSwiZml4ZWRfcHJpY2UiOnsicGlwZWxpbmUiOlsiYWdnIl0sInN0YWdlcyI6eyJhZ2ciOnsia2luZCI6ImFnZyIsIm5hbWUiOiJhdmciLCJwYXJhbWV0ZXJzIjp7ImRlZmF1bHQiOiJuYW4ifX19fX0sImNhc3RfdHlwZSI6ImZsb2F0IiwiZmVhdHVyZXMiOlsidG9waWNzM19zb3VyY2UuKiIsImIuKiIsImEucHJpY2UgYXMgZml4ZWRfcHJpY2UiXSwiZ3JvdXBfdHlwZSI6Ik1BUF9UWVBFIiwiaW5fcGFydGl0aW9uX2pvaW5fcnVsZSI6dHJ1ZSwibmFtZSI6InRvcGljcyIsInNvdXJjZSI6eyJqb2luX3J1bGUiOiJhIGZ1bGxfb3V0ZXIgKGIgbGVmdF9vdXRlciB0b3BpY3MzX3NvdXJjZSkiLCJuYW1lcyI6WyJ0b3BpY3MxX3NvdXJjZSBhcyBhIiwidG9waWNzMl9zb3VyY2UgYXMgYiIsInRvcGljczNfc291cmNlIl0sInRhYmxlcyI6W3siYWxpYXMiOiJ0b3BpY3MxX3NvdXJjZSIsImR0IjoiMjAyMC0wMy0yMSIsImV4cGVjdGVkX3VpZF90eXBlcyI6W10sImZlYXR1cmVzIjpbImZlYXR1cmVzWydwcmljZSddIGFzIHByaWNlIiwiZmVhdHVyZXMiXSwibmFtZSI6InRlc3RfZGIudG9waWNzX29uZSIsInBhcnRpdGlvbnMiOlt7ImR0IjoiMjAyMC0wMy0yMSIsInJlZ2lvbiI6Ijc5OSJ9XSwidWlkX2ltaXRhdGlvbiI6eyJ1aWQiOiJpZCIsInVpZF90eXBlIjoiRU1BSUwifSwid2hlcmUiOiJhZ2UgPiAyMSJ9LHsiYWxpYXMiOiJ0b3BpY3MyX3NvdXJjZSIsImR0IjoiMjAyMC0wMy0yOCIsImV4cGVjdGVkX3VpZF90eXBlcyI6WyJPS0lEIiwiVktJRCJdLCJuYW1lIjoidGVzdF9kYi50b3BpY3NfdHdvIiwicGFydGl0aW9ucyI6W3siZHQiOiIyMDIwLTAzLTI4IiwidWlkX3R5cGUiOiJPS0lEIn0seyJkdCI6IjIwMjAtMDMtMjgiLCJ1aWRfdHlwZSI6IlZLSUQifV19LHsiYWxpYXMiOiJ0b3BpY3MzX3NvdXJjZSIsImR0IjoiMjAyMC0wMy0yOCIsImV4cGVjdGVkX3VpZF90eXBlcyI6WyJPS0lEIiwiVktJRCJdLCJuYW1lIjoidGVzdF9kYi50b3BpY3NfdGhyZWUiLCJwYXJ0aXRpb25zIjpbeyJkdCI6IjIwMjAtMDMtMjgiLCJ1aWRfdHlwZSI6Ik9LSUQifSx7ImR0IjoiMjAyMC0wMy0yOCIsInVpZF90eXBlIjoiVktJRCJ9XX1dfX0seyJhZ2ciOnsiZG9tYWluIjp7InBpcGVsaW5lIjpbImFnZyJdLCJzdGFnZXMiOnsiYWdnIjp7ImtpbmQiOiJhZ2ciLCJuYW1lIjoiYXZnIiwicGFyYW1ldGVycyI6eyJkZWZhdWx0IjoibnVsbCJ9fX19fSwiY2FzdF90eXBlIjoiZG91YmxlIiwiZ3JvdXBfdHlwZSI6IkFSUkFZX1RZUEUiLCJpbl9wYXJ0aXRpb25fam9pbl9ydWxlIjp0cnVlLCJuYW1lIjoicHJvZnMiLCJzb3VyY2UiOnsibmFtZXMiOlsicHJvZnNfc291cmNlIl0sInRhYmxlcyI6W3siYWxpYXMiOiJwcm9mc19zb3VyY2UiLCJkdCI6IjIwMjAtMDMtMjgiLCJleHBlY3RlZF91aWRfdHlwZXMiOlsiT0tJRCIsIlZLSUQiXSwibmFtZSI6InRlc3RfZGIucHJvZnMiLCJwYXJ0aXRpb25zIjpbeyJkdCI6IjIwMjAtMDMtMjgiLCJ1aWRfdHlwZSI6Ik9LSUQifSx7ImR0IjoiMjAyMC0wMy0yOCIsInVpZF90eXBlIjoiVktJRCJ9XX1dfX0seyJhZ2ciOnsiZG9tYWluIjp7InBpcGVsaW5lIjpbImFnZyJdLCJzdGFnZXMiOnsiYWdnIjp7ImtpbmQiOiJhZ2ciLCJuYW1lIjoiYXZnIiwicGFyYW1ldGVycyI6eyJkZWZhdWx0IjoibnVsbCJ9fX19fSwiY2FzdF90eXBlIjoiZmxvYXQiLCJncm91cF90eXBlIjoiUFJFRklYX1RZUEUiLCJpbl9wYXJ0aXRpb25fam9pbl9ydWxlIjp0cnVlLCJuYW1lIjoiZ3JvdXBzIiwic291cmNlIjp7Im5hbWVzIjpbImdyb3Vwc19zb3VyY2UiXSwidGFibGVzIjpbeyJhbGlhcyI6Imdyb3Vwc19zb3VyY2UiLCJkdCI6IjIwMjAtMDMtMjgiLCJleHBlY3RlZF91aWRfdHlwZXMiOlsiT0tJRCJdLCJuYW1lIjoidGVzdF9kYi5ncm91cHMiLCJwYXJ0aXRpb25zIjpbeyJkdCI6IjIwMjAtMDMtMjgiLCJ1aWRfdHlwZSI6Ik9LSUQifV19XX19XSwiZHQiOiIyMDIwLTAzLTI4Iiwiam9pbl9ydWxlIjoidG9waWNzIGZ1bGxfb3V0ZXIgKHByb2ZzIGxlZnRfb3V0ZXIgZ3JvdXBzKSIsIm1hdGNoaW5nIjp7InRhYmxlIjp7ImR0IjoiMjAyMC0wMi0yMCIsImV4cGVjdGVkX3VpZF90eXBlcyI6W10sIm5hbWUiOiJjZG1faGlkLmhpZF92Ml9tYXRjaGluZyIsInBhcnRpdGlvbnMiOlt7ImR0IjoiMjAyMC0wMi0yMCIsInVpZDFfdHlwZSI6Ik9LSUQiLCJ1aWQyX3R5cGUiOiJISUQifSx7ImR0IjoiMjAyMC0wMi0yMCIsInVpZDFfdHlwZSI6IlZLSUQiLCJ1aWQyX3R5cGUiOiJISUQifV19LCJ1aWRfdHlwZXNfaW5wdXQiOlsiVktJRCIsIk9LSUQiXX0sInNodWZmbGVfcGFydGl0aW9ucyI6NTEyLCJ0YWJsZSI6InRlc3RfZGIudGFyZ2V0X3RhYmxlIiwidGFibGVfam9pbl9ydWxlIjoidG9waWNzIGZ1bGxfb3V0ZXIgKHByb2ZzIGxlZnRfb3V0ZXIgZ3JvdXBzKSIsInVpZF90eXBlIjoiSElEIn0=""")
    )
    b64Encoded.foreach{ case(len, text) =>
      val cfg = EtlFeatures.parseJsonEtlConfig(text.b64Decode)
      assert(cfg.domains.length === len)
    }
  }

  it should "parse empty join rule" in {
    JoinRule.enumerateItems(
      parseJoinRule(
        rule = "",
        defaultItem = "foo"
      )) should contain theSameElementsAs Seq("foo")
  }

  it should "parse single item in join rule" in {
    JoinRule.enumerateItems(parseJoinRule(
      rule = "bar",
      defaultItem = "foo"
    )) should contain theSameElementsAs Seq("bar")
  }

  it should "parse complex join rule" in {
    JoinRule.enumerateItems(parseJoinRule(
      rule  = "topics full_outer (profs left_outer groups)",
      defaultItem = "bar"
    )) should contain theSameElementsAs Seq("topics", "profs", "groups")
  }

  it should "select domains sources" in {
    import EtlFeaturesFunctionsTest.implicits._
    val all = Seq(
      DomainSourceDataFrame(domain = "foo", source = "bar", table = "baz", None),
      DomainSourceDataFrame(domain = "foo2", source = "bar2", table = "baz2", None),
      DomainSourceDataFrame(domain = "foo3", source = "bar3", table = "baz3", None)
    )

    EtlFeatures.selectDomainsSources(allSources = all, domains = Seq("foo")) should contain theSameElementsAs Seq(
      DomainSourceDataFrame(domain = "foo", source = "bar", table = "baz", None)
    )

    EtlFeatures.selectDomainsSources(allSources = all, domains = Seq("foo", "foo3")) should contain theSameElementsAs Seq(
      DomainSourceDataFrame(domain = "foo", source = "bar", table = "baz", None),
      DomainSourceDataFrame(domain = "foo3", source = "bar3", table = "baz3", None)
    )

    EtlFeatures.selectDomainsSources(allSources = all, domains = Seq("quiz")) should contain theSameElementsAs Seq()
  }

  it should "load selected DFs" in {
    import EtlFeaturesFunctionsTest.implicits._

    inputDF(spark).createOrReplaceTempView("baz")

    val sources: Seq[DomainSourceDataFrame] = Seq(
      DomainSourceDataFrame(domain = "foo", source = "bar", table = "baz", None)
    )

    EtlFeatures.loadTables(sources)(spark).foreach(x => {
      assert(x.df.nonEmpty)
      assert(x.df.get.count === 1)
      //x.df.get.show(truncate = false)
    })
  }

  it should "compute result with extra step (log) after" in {
    var console: String = ""
    def job(a: Int): String = (a + a).toString
    def sideEffect(b: String): Unit = console = b
    val res = EtlFeatures.resultWithLog(job(21), sideEffect("42"))
    assert(res === "42")
    assert(console === "42")
  }

  it should "build prefix domain" in {
    import DefaultSeparators._

    val df = Seq(SourceRow("uid: 42, uid_type: OKID, pFeature1: true, pFeature2: 2, pFeature3: 3")).toDF
      .selectCSVcols("uid,uid_type,pFeature1 as f1,pFeature2 as f2,pFeature3 as f3,pFeature4 as f4")

    val res = EtlFeatures.buildPrefixDomain("foo", df, "float")
    val expected = Seq("[42,OKID,1.0,2.0,3.0,null]")
    val actual = res.collect.map(_.toString)

    res.columns should contain theSameElementsInOrderAs "uid,uid_type,foo_f1,foo_f2,foo_f3,foo_f4".splitTrim
    actual should contain theSameElementsAs expected
  }

  it should "join domains" in {
    val joinRule = parseJoinRule("foo inner bar", "")

    val domains = Map(
      "foo" -> Seq(SourceRow("uid: 42, uid_type: OKID, mFeature: a=1;b=2")).toDF.selectCSVcols("uid,uid_type, mFeature as foo"),
      "bar" -> Seq(SourceRow("uid: 42, uid_type: OKID, aFeature: 3;4")).toDF.selectCSVcols("uid,uid_type, aFeature as bar")
    )

    val res = EtlFeatures.joinDomains(domains, joinRule)
    val expected = Seq("[42,OKID,Map(a -> 1, b -> 2),WrappedArray(3, 4)]")
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,IntegerType,true);" +
        "StructField(uid_type,StringType,true);" +
        "StructField(foo,MapType(StringType,IntegerType,true),true);" +
        "StructField(bar,ArrayType(IntegerType,true),true)"
    )

    actual should contain theSameElementsAs expected
  }

  it should "produce matching table" in {
    val cfg = TableConfig(
      name = "foo",
      dt = "2020-05-18",
      partitions = List(Map("dt" -> "2020-05-18")),
      expected_uid_types = None,
      where = None,
      alias = None, features = None, uid_imitation = None
    )
    val df = Seq(SourceRow("uid: 1, uid_type: OKID")).toDF
      .withColumn("dt", sf.lit("2020-05-18"))
      .withColumn("uid2_type", sf.lit("VKID")).withColumn("uid2", sf.lit(2))
      .withColumn("uid1", $"uid").withColumn("uid1_type", $"uid_type")

    val expected = Seq("MatchingTableRow(1,2,OKID,VKID)")
    // case class MatchingTableRow(uid1: String, uid2: String, uid1_type: String, uid2_type: String)

    val res = EtlFeatures.prepareMatchingTable(cfg, df)
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid1,StringType,true);" +
        "StructField(uid2,StringType,false);" +
        "StructField(uid1_type,StringType,true);" +
        "StructField(uid2_type,StringType,false)"
    )
    actual should contain theSameElementsAs expected
  }

  it should "load filtered matching table" in {
    val cfg = TableConfig(
      name = "foo",
      dt = "2020-05-19",
      partitions = List(
        Map("dt" -> "2020-05-19", "uid1_type" -> "OKID", "uid2_type" -> "HID"),
        Map("dt" -> "2020-05-19", "uid1_type" -> "VKID", "uid2_type" -> "HID")
      ),
      expected_uid_types = None,
      where = Some("id > 0"),
      alias = None, features = None, uid_imitation = None
    )
    val df = Seq(
      SourceRow("dt: 2020-05-19, id: 1, uid_type: OKID, uid: 2, pFeature2: HID"),
      SourceRow("dt: 2020-05-20, id: 1, uid_type: OKID, uid: 2, pFeature2: HID"),
      SourceRow("dt: 2020-05-19, id: 0, uid_type: OKID, uid: 2, pFeature2: HID"),
      SourceRow("dt: 2020-05-19, id: 1, uid_type: EMAIL, uid: 2, pFeature2: HID"),
      SourceRow("dt: 2020-05-19, id: 1, uid_type: OKID, uid: 2, pFeature2: VKID"),
      SourceRow("dt: 2020-05-19, id: 3, uid_type: VKID, uid: 4, pFeature2: HID")
    ).toDF.selectCSVcols("dt, id, id as uid1, uid as uid2, uid_type as uid1_type, pFeature2 as uid2_type")

    // case class MatchingTableRow(uid1: String, uid2: String, uid1_type: String, uid2_type: String)
    val expected = Seq(
      "MatchingTableRow(1,2,OKID,HID)",
      "MatchingTableRow(3,4,VKID,HID)"
    )

    val res = EtlFeatures.prepareMatchingTable(cfg, df)
    val actual = res.collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }

  it should "add missing columns" in {
    val target: DataFrame = Seq(
      SourceRow("dt: 2020-06-03, id: 42, uid_type: VKID, uid: 37, pFeature2: foo")
    ).toDF

    val res: DataFrame = EtlFeatures.addMissingColumns(
      target.selectCSVcols("pFeature1, id, aFeature"),
      target.schema
    )

    res.schema.fields should contain theSameElementsAs target.schema.fields

    assert(
      res.select(target.schema.fieldNames.map(sql.functions.col): _*).schema.mkString(";")
        ===
        target.schema.mkString(";")
    )
  }

  it should "return source after pseudo-join or empty join_rule" in {
    import spark.implicits._

    val tables: Map[String, DataFrame] = Map(
      "foo" -> Seq(SourceRow("dt: 2020-05-05, uid: 42, uid_type: OKID, pFeature1: true, pFeature2: foo")).toDF
    )

    // one table
    val rule = parseJoinRule(rule="foo", defaultItem="bar")

    val expected = Seq(
      "[2020-05-05,42,OKID,true,foo,null,null,null,null,null,null]"
    )

    val actual = EtlFeatures.joinWithAliases(tables, rule)
      .collect.map(_.toString)

    // empty join_rule
    actual should contain theSameElementsAs expected
    EtlFeatures.joinWithAliases(
      tables,
      joinTree=parseJoinRule(rule="", defaultItem="foo")
    ).collect.map(_.toString) should contain theSameElementsAs expected
  }

  it should "join few sources" in {
    import spark.implicits._

    val tables: Map[String, DataFrame] = Map(
      "a" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature1: true")).toDF.selectCSVcols( "uid,uid_type,pFeature1"),
      "b" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature2: 2")).toDF.selectCSVcols(    "uid,uid_type,pFeature2"),
      "c" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature3: 3")).toDF.selectCSVcols(    "uid,uid_type,pFeature3"),
      "d" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature4: 4")).toDF.selectCSVcols(    "uid,uid_type,pFeature4")
    )

    val rule = parseJoinRule("a outer (b left_outer c) inner d", "")

    val expected: Seq[String] = Seq(
      "[42,OKID,true,2,3,4.0]"
    )

    val actual = EtlFeatures.joinWithAliases(tables, rule).collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }

  it should "assign aliases to joined dfs" in {
    import spark.implicits._

    val tables: Map[String, DataFrame] = Map(
      "a" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature1: true")).toDF.selectCSVcols( "uid,uid_type,pFeature1"),
      "b" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature2: 2")).toDF.selectCSVcols(    "uid,uid_type,pFeature2"),
      "c" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature3: 3")).toDF.selectCSVcols(    "uid,uid_type,pFeature3"),
      "d" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature4: 4")).toDF.selectCSVcols(    "uid,uid_type,pFeature4")
    )

    val rule = parseJoinRule("a inner b inner c inner d", "")

    val df = EtlFeatures.joinWithAliases(tables, rule)
    def expr(e: String) = df.select(sql.functions.expr(e)).collect.map(_.toString)

    expr("a.*") should contain theSameElementsAs Seq("[42,OKID,true]")
    expr("b.*") should contain theSameElementsAs Seq("[2]")
    expr("c.*") should contain theSameElementsAs Seq("[3]")
    expr("d.*") should contain theSameElementsAs Seq("[4.0]")
  }

  it should "perform different joins" in {
    // inner, full_outer, left_outer, right_outer, left_semi, left_anti,
    // no `cross` while using columns (natural join)
    import spark.implicits._

    val tables: Map[String, DataFrame] = Map(
      "a" -> Seq(
        SourceRow("uid: 41, uid_type: OKID, pFeature1: true"),
        SourceRow("uid: 42, uid_type: OKID, pFeature1: true")
      ).toDF.selectCSVcols( "uid,uid_type,pFeature1"),
      "b" -> Seq(
        SourceRow("uid: 42, uid_type: OKID, pFeature2: 2"),
        SourceRow("uid: 43, uid_type: OKID, pFeature2: 2")
      ).toDF.selectCSVcols("uid,uid_type,pFeature2")
    )

    def join(rule: String): Seq[String] = EtlFeatures.joinWithAliases(tables, parseJoinRule(rule, "")).collect.map(_.toString).toSeq

    join("a left_anti b") should contain theSameElementsAs Seq("[41,OKID,true]")
    join("a left_semi b") should contain theSameElementsAs Seq("[42,OKID,true]")
    join("a right_outer b") should contain theSameElementsAs Seq("[42,OKID,true,2]", "[43,OKID,null,2]")
    join("a left_outer b") should contain theSameElementsAs Seq("[41,OKID,true,null]", "[42,OKID,true,2]")
    join("a full_outer b") should contain theSameElementsAs Seq("[41,OKID,true,null]", "[42,OKID,true,2]", "[43,OKID,null,2]")
    join("a inner b") should contain theSameElementsAs Seq("[42,OKID,true,2]")
  }

  it should "perform join in given order" in {
    import spark.implicits._

    val tables: Map[String, DataFrame] = Map(
      "a" -> Seq(
        SourceRow("uid: 41, uid_type: OKID, pFeature1: true"),
        SourceRow("uid: 42, uid_type: OKID, pFeature1: true")
      ).toDF.selectCSVcols("uid,uid_type,pFeature1"),

      "b" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature2: 2")).toDF.selectCSVcols("uid,uid_type,pFeature2"),
      "c" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature3: 3")).toDF.selectCSVcols("uid,uid_type,pFeature3"),

      "d" -> Seq(
        SourceRow("uid: 41, uid_type: OKID, pFeature4: 4"),
        SourceRow("uid: 42, uid_type: OKID, pFeature4: 4")
      ).toDF.selectCSVcols("uid,uid_type,pFeature4")
    )

    val rule = parseJoinRule("(a left_outer b) inner (c right_outer d)", "")

    val expected: Seq[String] = Seq(
      "[41,OKID,true,null,null,4.0]",
      "[42,OKID,true,2,3,4.0]"
    )

    val actual = EtlFeatures.joinWithAliases(tables, rule).collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }

  it should "build null map domain from array cols" in {
    val df = Seq(SourceRow("uid: 42, uid_type: OKID, aFeature: 1;2;3")).toDF
      .selectCSVcols("cast(uid as string) uid, uid_type, aFeature as f1, aFeature as f2")

    val res = EtlFeatures.buildMapDomain("foo", df, "float")
    val expected = Seq("[42,OKID,null]")
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,StringType,true);" +
        "StructField(uid_type,StringType,true);" +
        "StructField(foo,MapType(StringType,FloatType,true),true)"
    )
    actual should contain theSameElementsAs expected
  }

  it should "build map domain from primitive cols" in {
    val df = Seq(SourceRow("uid: 42, uid_type: OKID, pFeature1: true, pFeature3: 3")).toDF
      .selectCSVcols("uid,uid_type, pFeature1 as f1, pFeature2 as f2, pFeature3 as f3")

    val res = EtlFeatures.buildMapDomain("bar", df, "float")
    val expected = Seq("[42,OKID,Map(f1 -> 1.0, f3 -> 3.0)]")
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,IntegerType,true);" +
        "StructField(uid_type,StringType,true);" +
        "StructField(bar,MapType(StringType,FloatType,true),true)"
    )
    actual should contain theSameElementsAs expected
  }

  it should "build map domain from map cols" in {
    val df = Seq(SourceRow("uid: 42, uid_type: OKID, mFeature: a=1;b=2;c=3, pFeature2: d, pFeature3: 4")).toDF
      .selectCSVcols("uid,uid_type, mFeature as m1, mFeature as m2, pFeature2 as f3, pFeature3 as f4")
      .withColumn("m3", sf.map(sf.col("f3"), sf.col("f4")))
      .drop("f3", "f4")

    val res = EtlFeatures.buildMapDomain("baz", df, "float")

    val expected = Seq("[42,OKID,Map(a -> 1.0, b -> 2.0, c -> 3.0, d -> 4.0)]")
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,IntegerType,true);" +
        "StructField(uid_type,StringType,true);" +
        "StructField(baz,MapType(StringType,FloatType,true),true)"
    )
    actual should contain theSameElementsAs expected
  }

  it should "build map domain from mixed cols" in {
    val df = Seq(SourceRow("uid: 42, uid_type: OKID, mFeature: a=1;b=2;c=3, aFeature: 9;8;7, pFeature2: 2, pFeature3: 3")).toDF
      .selectCSVcols("uid,uid_type, mFeature as m1, aFeature as a1, pFeature1 as f1, pFeature2 as f2, pFeature3 as f3")

    val res = EtlFeatures.buildMapDomain("quix", df, "float")
    val expected = Seq("[42,OKID,Map(a -> 1.0, f3 -> 3.0, b -> 2.0, c -> 3.0, f2 -> 2.0)]")
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,IntegerType,true);" +
        "StructField(uid_type,StringType,true);" +
        "StructField(quix,MapType(StringType,FloatType,true),true)"
    )

    actual should contain theSameElementsAs expected
  }

  it should "build map domain dropping nulls on merge" in {
    val df = inputDF(spark).withColumn("uid", sf.lit("42")).withColumn("uid_type", sf.lit("OKID"))
    val res = EtlFeatures.buildMapDomain("quuz", df, "float")
    val expected = Seq("[42,OKID,Map(s42 -> 42.0, d42 -> 142.0, f42 -> 242.0)]")
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,StringType,false);" +
        "StructField(uid_type,StringType,false);" +
        "StructField(quuz,MapType(StringType,FloatType,true),true)"
    )

    actual should contain theSameElementsAs expected
  }

  it should "build map domain w/o duplicated keys" in {
    val df = Seq(SourceRow("uid: 42, uid_type: OKID, mFeature: a=1;b=2;c=3")).toDF
      .selectCSVcols("uid;uid_type; mFeature as m1; mFeature as m2; map('d', 4) as m3", sep = ";")

    val res = EtlFeatures.buildMapDomain("baz", df, "float").persist(StorageLevel.MEMORY_ONLY)

    res.select(sf.map_keys(res("baz")))
      .collect.map(_.toString) should contain theSameElementsAs Seq("[WrappedArray(a, b, c, d)]")

    val expected = Seq("[42,OKID,[a -> 1.0, b -> 2.0, c -> 3.0, d -> 4.0]]")

    val actual: Array[String] = {
      val castCols = res.schema.map { field => sf.col(field.name).cast("string") }

      res
        .select(castCols: _*)
        .collect
        .map(_.toString)
    }

    assert(res.schema.mkString(";") ===
      "StructField(uid,IntegerType,true);" +
        "StructField(uid_type,StringType,true);" +
        "StructField(baz,MapType(StringType,FloatType,true),true)"
    )

    actual should contain theSameElementsAs expected
  }

  it should "build null map domain from empty collection" in {
    val df = Seq(
      SourceRow("uid: 42, uid_type: OKID, mFeature: a=null;b=null;c=null"),
      SourceRow("uid: 43, uid_type: VKID, mFeature: ()")
    ).toDF
      .selectCSVcols(
        "uid;uid_type; mFeature as m1; mFeature as m2; cast(map() as map<string,double>) as m3",
        sep = ";"
      )

    val res = EtlFeatures.buildMapDomain("baz", df, "float")

    val expected = Seq(
      "[42,OKID,null]",
      "[43,VKID,null]"
    )

    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,IntegerType,true);" +
        "StructField(uid_type,StringType,true);" +
        "StructField(baz,MapType(StringType,FloatType,true),true)"
    )

    actual should contain theSameElementsAs expected
  }

  it should "build null map domain from null cols" in {
    val df = Seq(SourceRow("uid: 42, uid_type: OKID")).toDF
      .selectCSVcols(
        "uid;uid_type; cast(null as map<string,int>) as m1; cast(null as map<string,double>) as m2",
        sep = ";"
      )

    val res = EtlFeatures.buildMapDomain("baz", df, "float")
    val expected = Seq("[42,OKID,null]")
    val actual = res.collect.map(_.toString)

    assert(res.schema.mkString(";") ===
      "StructField(uid,IntegerType,true);" +
        "StructField(uid_type,StringType,true);" +
        "StructField(baz,MapType(StringType,FloatType,true),true)"
    )

    actual should contain theSameElementsAs expected
  }

  it should "map uids to self w/o self-mapping in cross-table" in {
    // VKID => VKID
    // retain not mapped original VKID

    val inTypes = "OKID, VKID".s2list
    val outType = "VKID"

    val df: DataFrame = {
      val csv =
        """
          |uid, uid_type, foo, bar
          |1, VKID,   11, 12
          |2, VKID,   21, 22
          |""".stripMargin.trim

      dfFromCSV(csv)
    }
    val cross = {
      val csv =
        """
          |uid1, uid2, uid1_type, uid2_type
          |1, 101, VKID, VKID
          |""".stripMargin.trim

      dfFromCSV(csv).as[MatchingTableRow]
    }
    val expected = {
      val csv =
        """
          |uid, uid_type, foo, bar
          |101, VKID, 11, 12
          |1,   VKID, 11, 12
          |2,   VKID, 21, 22
          |""".stripMargin.trim

      dfFromCSV(csv).setColumnsInOrder(withDT = false, withUT = true)
    }

    val res = EtlFeatures.mapUids(df, cross, inTypes, outType)(identity)
    val actual = res.collect

    assert(actual.length === 3)
    actual should contain theSameElementsAs expected.collect
  }

  it should "map uids to self with self-mapping in cross-table" in {
    // OKID, VKID => VKID
    // don't add extra records

    val inTypes = "OKID, VKID".s2list
    val outType = "VKID"

    val df: DataFrame = {
      val csv =
        """
          |uid, uid_type, foo, bar
          |1, VKID,   11, 12
          |2, VKID,   21, 22
          |""".stripMargin.trim

      dfFromCSV(csv)
    }
    val cross = {
      val csv =
        """
          |uid1, uid2, uid1_type, uid2_type
          |1, 101, VKID, VKID
          |1, 1,   VKID, VKID
          |2, 2,   VKID, VKID
          |""".stripMargin.trim

      dfFromCSV(csv).as[MatchingTableRow]
    }
    val expected = {
      val csv =
        """
          |uid, uid_type, foo, bar
          |101, VKID, 11, 12
          |1,   VKID, 11, 12
          |2,   VKID, 21, 22
          |""".stripMargin.trim

      dfFromCSV(csv).setColumnsInOrder(withDT = false, withUT = true)
    }

    val res = EtlFeatures.mapUids(df, cross, inTypes, outType)(identity)
    val actual = res.collect

    assert(actual.length === 3)
    actual should contain theSameElementsAs expected.collect
  }

  it should "map uids to same uid_type, no self-mapping in cross-table" in {
    // OKID, VKID => VKID
    // retain not mapped original VKID

    val inTypes = "OKID, VKID".s2list
    val outType = "VKID"

    val df: DataFrame = {
      val csv =
        """
          |uid, uid_type, foo, bar
          |1, OKID,   11, 12
          |1, VKID,   21, 22
          |2, OKID,   31, 32
          |2, VKID,   41, 42
          |3, OKID,   51, 52
          |4, VKID,   61, 62
          |""".stripMargin.trim

      dfFromCSV(csv)
    }
    val cross = {
      val csv =
        """
          |uid1, uid2, uid1_type, uid2_type
          |1, 101, OKID, VKID
          |1, 101, VKID, VKID
          |2, 102, OKID, VKID
          |2, 103, OKID, VKID
          |2, 103, VKID, VKID
          |3, 104, VKID, VKID
          |4, 105, OKID, VKID
          |""".stripMargin.trim

      dfFromCSV(csv).as[MatchingTableRow]
    }
    val expected = {
      val csv =
        """
          |uid, uid_type, foo, bar
          |101, VKID, 11, 12
          |1,   VKID, 21, 22
          |101, VKID, 21, 22
          |102, VKID, 31, 32
          |103, VKID, 31, 32
          |2,   VKID, 41, 42
          |103, VKID, 41, 42
          |4,   VKID, 61, 62
          |""".stripMargin.trim

      dfFromCSV(csv).setColumnsInOrder(withDT = false, withUT = true)
    }

    val res = EtlFeatures.mapUids(df, cross, inTypes, outType)(identity)
    val actual = res.collect

    assert(actual.length === 8)
    actual should contain theSameElementsAs expected.collect
  }

  it should "map uids to same uid_type, with self-mapping in cross-table" in {
    // OKID, VKID => VKID
    // don't add extra records

    val inTypes = "OKID, VKID".s2list
    val outType = "VKID"

    val df: DataFrame = {
      val csv =
        """
          |uid, uid_type, foo, bar
          |1, OKID,   11, 12
          |2, VKID,   21, 22
          |""".stripMargin.trim

      dfFromCSV(csv)
    }
    val cross = {
      val csv =
        """
          |uid1, uid2, uid1_type, uid2_type
          |1, 101,  OKID, VKID
          |2, 2,    VKID, VKID
          |2, 102,  VKID, VKID
          |""".stripMargin.trim

      dfFromCSV(csv).as[MatchingTableRow]
    }
    val expected = {
      val csv =
        """
          |uid, uid_type, foo, bar
          |101, VKID, 11, 12
          |2,   VKID, 21, 22
          |102, VKID, 21, 22
          |""".stripMargin.trim

      dfFromCSV(csv).setColumnsInOrder(withDT = false, withUT = true)
    }

    val res = EtlFeatures.mapUids(df, cross, inTypes, outType)(identity)
    val actual = res.collect

    assert(actual.length === 3)
    actual should contain theSameElementsAs expected.collect
  }

  it should "map uids to different uid_type" in {

    val inTypes = "OKID, VKID".s2list
    val outType = "HID"

    val df: DataFrame = {
      val csv =
        """
          |uid, uid_type, foo, bar
          |1, OKID,   11, 12
          |2, VKID,   21, 22
          |3, OKID,   31, 32
          |4, VKID,   41, 42
          |""".stripMargin.trim

      dfFromCSV(csv)
    }
    val cross = {
      val csv =
        """
          |uid1, uid2, uid1_type, uid2_type
          |1, 101, OKID, HID
          |3, 101, OKID, HID
          |2, 102, VKID, HID
          |2, 103, VKID, HID
          |4, 104, OKID, HID
          |5, 105, VKID, HID
          |""".stripMargin.trim

      dfFromCSV(csv).as[MatchingTableRow]
    }
    val expected = {
      val csv =
        """
          |uid, uid_type, foo, bar
          |101, HID, 11, 12
          |101, HID, 31, 32
          |102, HID, 21, 22
          |103, HID, 21, 22
          |""".stripMargin.trim

      dfFromCSV(csv).setColumnsInOrder(withDT = false, withUT = true)
    }

    val res = EtlFeatures.mapUids(df, cross, inTypes, outType)(identity)
    val actual = res.collect

    assert(actual.length === 4)
    actual should contain theSameElementsAs expected.collect
  }

  it should "prepare simple source" in {
    import spark.implicits._

    val data = DomainSourceDataFrame(
      domain = "profs",
      source = NameWithAlias("profs_source", "profs_source"),
      table = NameWithAlias("snb_db.profs_tab", "profs_source"),
      df = Some(Seq(
        SourceRow("dt: 2020-05-06, uid: 1, uid_type: VKID, pFeature1: true, pFeature2: 2, pFeature3: 3")
      ).toDF)
    )

    val cfg: TableConfig = TableConfig(
      name = "snb_db.profs_tab",
      alias = Some("profs_source"),
      dt = "2020-05-06",
      partitions = List(Map("dt" -> "2020-05-06", "uid_type" -> "VKID")),
      expected_uid_types = Some(List("VKID")),
      features = None,
      uid_imitation = None,
      where = None
    )

    val expected = Seq("[1,VKID,true,2,3,null,null,null,null,null]")  // Seq(("a", "b")).toDF.collect
    val res = EtlFeatures.prepareSource(data, cfg)

    res.df.get.collect.map(_.toString) should contain theSameElementsAs expected
  }

  it should "filter dt while preparing source" in {
    import spark.implicits._

    val tabname = "snb_db.groups_tab"
    val sourcename = "groups_source"

    val data = DomainSourceDataFrame(
      domain = "groups",
      source = NameWithAlias(sourcename, "ga"),
      table = NameWithAlias(tabname, sourcename),
      df = Some(Seq(
        SourceRow("dt: 2020-05-06, uid: 10, uid_type: VKID"),
        SourceRow("dt: 2020-05-05, uid: 21, uid_type: VKID"),
        SourceRow("dt: 2020-05-05, uid: 22, uid_type: OKID"),
        SourceRow("dt: 2020-05-04, uid: 30, uid_type: VKID")
      ).toDF)
    )

    val cfg: TableConfig = TableConfig(
      name = tabname, alias = Some(sourcename),
      dt = "2020-05-05",
      partitions = List(Map("dt" -> "2020-05-05", "uid_type" -> "OKID")),
      expected_uid_types = Some(List("OKID")),
      features = None, uid_imitation = None, where = None
    )

    val expected = Seq("[22,OKID,null,null,null,null,null,null,null,null]")
    val actual = EtlFeatures.prepareSource(data, cfg).df
      .get.collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }

  it should "filter partitions while preparing source" in {
    import spark.implicits._

    val tabname = "snb_db.topics_tab"
    val sourcename = "topics_source"

    val data = DomainSourceDataFrame(
      domain = "topics",
      source = NameWithAlias(sourcename, "ta"),
      table = NameWithAlias(tabname, sourcename),
      df = Some(Seq(
        SourceRow("dt: 2020-05-06, uid: 10, uid_type: VKID"),
        SourceRow("dt: 2020-05-05, uid: 21, uid_type: VKID"),
        SourceRow("dt: 2020-05-05, uid: 22, uid_type: OKID, pFeature1: true"),
        SourceRow("dt: 2020-05-04, uid: 30, uid_type: VKID")
      ).toDF)
    )

    val cfg: TableConfig = TableConfig(
      name = tabname, alias = Some(sourcename),
      dt = "2020-05-05",
      partitions = List(
        Map("dt" -> "2020-05-05", "uid_type" -> "VKID"),
        Map("dt" -> "2020-05-05", "uid_type" -> "OKID", "pFeature1" -> "true")
      ),
      expected_uid_types = Some(List("VKID", "OKID")),
      features = None, uid_imitation = None, where = None
    )

    // dropped columns: dt, pFeature1
    val expected = Seq(
      "[21,VKID,null,null,null,null,null,null,null]",
      "[22,OKID,null,null,null,null,null,null,null]"
    )

    val actual = EtlFeatures.prepareSource(data, cfg).df
      .get.collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }

  it should "filter using where expr. while preparing source" in {
    import spark.implicits._

    val tabname = "snb_db.topics_tab"
    val sourcename = "topics_source"

    val data = DomainSourceDataFrame(
      domain = "topics",
      source = NameWithAlias(sourcename, "ta"),
      table = NameWithAlias(tabname, sourcename),
      df = Some(Seq(
        SourceRow("dt: 2020-05-06, uid: 10, uid_type: VKID"),
        SourceRow("dt: 2020-05-05, uid: 21, uid_type: VKID, pFeature2: foo"),
        SourceRow("dt: 2020-05-05, uid: 22, uid_type: OKID, pFeature1: true"),
        SourceRow("dt: 2020-05-04, uid: 30, uid_type: VKID")
      ).toDF)
    )

    val cfg: TableConfig = TableConfig(
      name = tabname, alias = Some(sourcename),
      dt = "2020-05-05",
      partitions = List(
        Map("dt" -> "2020-05-05", "uid_type" -> "VKID"),
        Map("dt" -> "2020-05-05", "uid_type" -> "OKID", "pFeature1" -> "true")
      ),
      expected_uid_types = Some(List("VKID", "OKID")),
      features = None, uid_imitation = None,
      where = Some("pFeature2 = 'foo'")
    )

    val expected = Seq("[21,VKID,foo,null,null,null,null,null,null]")

    val actual = EtlFeatures.prepareSource(data, cfg).df
      .get.collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }

  it should "imitate (uid, uid_type) while preparing source" in {
    import spark.implicits._

    val tabname = "snb_db.foo_tab"
    val sourcename = "foo_source"

    val data = DomainSourceDataFrame(
      domain = "foo",
      source = NameWithAlias(sourcename, "fa"),
      table = NameWithAlias(tabname, sourcename),
      df = Some(Seq(
        SourceRow("dt: 2020-05-06, id: 10, pFeature1: true"),
        SourceRow("dt: 2020-05-05, id: 21, pFeature1: true, pFeature2: foo"),
        SourceRow("dt: 2020-05-05, id: 22, pFeature1: true"),
        SourceRow("dt: 2020-05-04, id: 30, pFeature1: true")
      ).toDF)
    )

    val cfg: TableConfig = TableConfig(
      name = tabname, alias = Some(sourcename),
      dt = "2020-05-05",
      partitions = List(Map("dt" -> "2020-05-05", "pFeature1" -> "true")),
      expected_uid_types = None,
      where = Some("pFeature2 = 'foo' or id = 22"),
      uid_imitation = Some(UidImitationConfig(uid = "id", uid_type = "VID")),
      features = None
    )

    val expected = Seq(
      "[foo,null,null,null,null,null,21,VID]",
      "[null,null,null,null,null,null,22,VID]"
    )

    val actual = EtlFeatures.prepareSource(data, cfg).df
      .get.collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }

  it should "drop invalid OKID,VKID while preparing source" in {
    import spark.implicits._

    val tabname = "snb_db.bar_tab"
    val sourcename = "bar_source"

    val data = DomainSourceDataFrame(
      domain = "bar",
      source = NameWithAlias(sourcename, "ba"),
      table = NameWithAlias(tabname, sourcename),
      df = Some(Seq(
        SourceRow("dt: 2020-05-05, uid: -1, uid_type: OKID"),
        SourceRow("dt: 2020-05-05, uid:  0, uid_type: VKID"),
        SourceRow("dt: 2020-05-05,          uid_type: OKID"),
        SourceRow("dt: 2020-05-05,          uid_type: VKID"),
        SourceRow("dt: 2020-05-05, uid:  1, uid_type: OKID"),
        SourceRow("dt: 2020-05-05, uid:  2, uid_type: VKID"),
        SourceRow("dt: 2020-05-05, uid: -1, uid_type: VID"),
        SourceRow("dt: 2020-05-05, uid:  0, uid_type: XID"),
        SourceRow("dt: 2020-05-05,          uid_type: ZID")
      ).toDF)
    )

    val cfg: TableConfig = TableConfig(
      name = tabname, alias = Some(sourcename),
      dt = "2020-05-05",
      partitions = List(Map("dt" -> "2020-05-05")),
      expected_uid_types = None,
      where = None, uid_imitation = None, features = None
    )

    val expected = Seq(
      "[1,OKID,null,null,null,null,null,null,null,null]",
      "[2,VKID,null,null,null,null,null,null,null,null]",
      "[-1,VID,null,null,null,null,null,null,null,null]",
      "[0,XID,null,null,null,null,null,null,null,null]"
    )

    val actual = EtlFeatures.prepareSource(data, cfg).df
      .get.collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }

  it should "process bigint VKID, OKID uid values" in {

    val tabname = "snb_db.bar_tab"
    val sourcename = "bar_source"

    val df: DataFrame = {
      val csv =
        """
          |uid, uid_type, dt, foo, bar
          |569146304067,  OKID, 2020-06-26, 11, 12
          |1580880987295, VKID, 2020-06-26, 21, 22
          |-157232184307, VKID, 2020-06-26, 31, 32
          |-140451212286, OKID, 2020-06-26, 41, 42
          |""".stripMargin.trim

      dfFromCSV(csv)
    }

    val data = DomainSourceDataFrame(
      domain = "bar",
      source = NameWithAlias(sourcename, "b"),
      table = NameWithAlias(tabname, sourcename),
      df = Some(df)
    )

    val cfg = TableConfig(
      name = tabname, alias = Some(sourcename),
      dt = "2020-06-26",
      partitions = List(
        Map("dt" -> "2020-06-26", "uid_type" -> "OKID"),
        Map("dt" -> "2020-06-26", "uid_type" -> "VKID")
      ),
      expected_uid_types = Some(List("OKID", "VKID")),
      where = None, uid_imitation = None, features = None
    )

    val expected = Seq(
      "[569146304067,OKID,11,12]",
      "[1580880987295,VKID,21,22]"
    )

    val actual = EtlFeatures.prepareSource(data, cfg).df
      .get.collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }

  it should "select features while preparing source" in {
    import spark.implicits._

    val tabname = "snb_db.baz_tab"
    val sourcename = "baz_source"

    val data = DomainSourceDataFrame(
      domain = "baz",
      source = NameWithAlias(sourcename, "ba"),
      table = NameWithAlias(tabname, sourcename),
      df = Some(Seq(
        SourceRow("dt: 2020-05-05, uid: 42, uid_type: OKID, pFeature1: true, pFeature2: foo")
      ).toDF)
    )

    val cfg: TableConfig = TableConfig(
      name = tabname, alias = Some(sourcename),
      dt = "2020-05-05",
      partitions = List(Map("dt" -> "2020-05-05", "uid_type" -> "OKID")),
      expected_uid_types = Some(List("OKID")),
      features = Some(List("pFeature1 as bf", "pFeature2 as sf")),
      where = None,
      uid_imitation = None
    )

    val expected = Seq(
      "[42,OKID,true,foo]"
    )

    val res = EtlFeatures.prepareSource(data, cfg).df.get
    val actual = res.collect.map(_.toString)

    actual should contain theSameElementsAs expected

    res.schema.map(_.name) should contain theSameElementsAs Seq(
      "uid", "uid_type", "bf", "sf"
    )
  }

  it should "make domain source from one source" in {
    val cfg = DomainConfig(
      name = "foo",
      source = SourceConfig(
        names = List("foo_src as a"),
        tables = List(TableConfig(name = "bar", dt = "2020-05-09", partitions = List(), None, None, None, None, None )),
        join_rule = None
      ),
      group_type = None, cast_type = None, features = None, agg = None
    )
    val sources = Seq(
      DomainSourceDataFrame(
        domain = "foo", source = NameWithAlias("foo_src", "a"), table = NameWithAlias("bar", "foo_src"),
        df = Some(Seq(
          SourceRow("uid: 42, uid_type: OKID, pFeature2: 2")
        ).toDF.selectCSVcols("uid,uid_type,pFeature2")))
    )
    val expected = Seq("[42,OKID,2]")

    val res: DomainSourceDataFrame = EtlFeatures.makeDomainSource(cfg, sources)
    val actual = res.df.get.collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }

  it should "make domain source from one source with features selection" in {
    val cfg = DomainConfig(
      name = "foo",
      source = SourceConfig(
        names = List("foo_src as a"),
        tables = List(TableConfig(name = "bar", dt = "2020-05-09", partitions = List(), None, None, None, None, None )),
        join_rule = None
      ),
      features = Some(List("a.pFeature2 as f2", "a.pFeature1 as f1")),
      group_type = None, cast_type = None, agg = None
    )
    val sources = Seq(
      DomainSourceDataFrame(
        domain = "foo", source = NameWithAlias("foo_src", "a"), table = NameWithAlias("bar", "foo_src"),
        df = Some(Seq(
          SourceRow("uid: 42, uid_type: OKID, pFeature1: true, pFeature2: 2, pFeature3: 3")
        ).toDF.selectCSVcols("uid,uid_type,pFeature1,pFeature2,pFeature3")))
    )
    val expected = Seq("[42,OKID,2,true]")

    val res = EtlFeatures.makeDomainSource(cfg, sources).df.get
    val actual = res.collect.map(_.toString)

    res.schema.map(_.name) should contain theSameElementsInOrderAs Seq("uid", "uid_type", "f2", "f1")
    actual should contain theSameElementsAs expected
  }

  it should "make domain source from three sources" in {
    val cfg = DomainConfig(
      name = "foo",
      source = SourceConfig(
        names = List("foo_src as a", "foo_src2", "foo_src3"),
        join_rule = Some("a inner foo_src2 inner foo_src3"),
        tables = List()
      ),
      features = None, group_type = None, cast_type = None, agg = None
    )
    val sources = Seq(
      DomainSourceDataFrame(
        domain = "foo", source = NameWithAlias("foo_src", "a"), table = NameWithAlias("", ""), df = Some(Seq(
          SourceRow("uid: 42, uid_type: OKID, pFeature1: true"),SourceRow("uid: 41, uid_type: OKID")
        ).toDF.selectCSVcols("uid,uid_type,pFeature1"))),
      DomainSourceDataFrame(
        domain = "foo", source = NameWithAlias("foo_src2", "foo_src2"), table = NameWithAlias("", ""), df = Some(Seq(
          SourceRow("uid: 42, uid_type: OKID, pFeature2: 2"),SourceRow("uid: 422, uid_type: OKID")
        ).toDF.selectCSVcols("uid,uid_type,pFeature2"))),
      DomainSourceDataFrame(
        domain = "foo", source = NameWithAlias("foo_src3", "foo_src3"), table = NameWithAlias("", ""), df = Some(Seq(
          SourceRow("uid: 42, uid_type: OKID, pFeature3: 3"),SourceRow("uid: 43, uid_type: OKID")
        ).toDF.selectCSVcols("uid,uid_type,pFeature3")))
    )
    val expected = Seq("[42,OKID,true,2,3]")

    val res = EtlFeatures.makeDomainSource(cfg, sources).df.get
    val actual = res.collect.map(_.toString)

    res.schema.map(_.name) should contain theSameElementsInOrderAs Seq("uid", "uid_type", "pFeature1", "pFeature2", "pFeature3")
    actual should contain theSameElementsAs expected
  }

  it should "make domain source from three sources with features selection" in {
    val cfg = DomainConfig(
      name = "foo",
      source = SourceConfig(
        names = List("foo_src as a", "foo_src2 as b", "foo_src3 as c"),
        join_rule = Some("a inner b inner c"),
        tables = List()
      ),
      features = Some(List("a.*", "c.*", "b.*")),
      group_type = None, cast_type = None, agg = None
    )

    val sources = Seq(
      DomainSourceDataFrame(
        domain = "foo", source = NameWithAlias("foo_src", "a"), table = NameWithAlias("", ""), df = Some(Seq(
          SourceRow("uid: 42, uid_type: OKID, pFeature1: true"),SourceRow("uid: 41, uid_type: OKID")
        ).toDF.selectCSVcols("uid,uid_type,pFeature1"))),
      DomainSourceDataFrame(
        domain = "foo", source = NameWithAlias("foo_src2", "b"), table = NameWithAlias("", ""), df = Some(Seq(
          SourceRow("uid: 42, uid_type: OKID, pFeature2: 2"),SourceRow("uid: 422, uid_type: OKID")
        ).toDF.selectCSVcols("uid,uid_type,pFeature2"))),
      DomainSourceDataFrame(
        domain = "foo", source = NameWithAlias("foo_src3", "c"), table = NameWithAlias("", ""), df = Some(Seq(
          SourceRow("uid: 42, uid_type: OKID, pFeature3: 3"),SourceRow("uid: 43, uid_type: OKID")
        ).toDF.selectCSVcols("uid,uid_type,pFeature3")))
    )

    val expectedFields = "uid,uid_type,pFeature1,pFeature3,pFeature2".splitTrim
    val expectedValues = Seq("[42,OKID,true,3,2]")

    val res = EtlFeatures.makeDomainSource(cfg, sources).df.get
    show(res, "domain source from 3 tables", force = true)

    val actualFields = res.schema.map(_.name)
    val actualValues = res.collect.map(_.toString)

    actualFields should contain theSameElementsInOrderAs expectedFields
    actualValues should contain theSameElementsAs expectedValues

  }

  it should "make domain source from three sources with individual features selection" in {
    val cfg = DomainConfig(
      name = "foo",
      source = SourceConfig(
        names = List("foo_src as a", "foo_src2 as b", "foo_src3 as c"),
        join_rule = Some("a inner b inner c"),
        tables = List()
      ),
      features = Some(List("c.pFeature3 as f3", "b.pFeature2 as f2", "a.pFeature1 as f1")),
      group_type = None, cast_type = None, agg = None
    )
    val sources = Seq(
      DomainSourceDataFrame(
        domain = "foo", source = NameWithAlias("foo_src", "a"), table = NameWithAlias("", ""), df = Some(Seq(
          SourceRow("uid: 42, uid_type: OKID, pFeature1: true"),SourceRow("uid: 41, uid_type: OKID")
        ).toDF.selectCSVcols("uid,uid_type,pFeature1"))),
      DomainSourceDataFrame(
        domain = "foo", source = NameWithAlias("foo_src2", "b"), table = NameWithAlias("", ""), df = Some(Seq(
          SourceRow("uid: 42, uid_type: OKID, pFeature2: 2"),SourceRow("uid: 422, uid_type: OKID")
        ).toDF.selectCSVcols("uid,uid_type,pFeature2"))),
      DomainSourceDataFrame(
        domain = "foo", source = NameWithAlias("foo_src3", "c"), table = NameWithAlias("", ""), df = Some(Seq(
          SourceRow("uid: 42, uid_type: OKID, pFeature3: 3"),SourceRow("uid: 43, uid_type: OKID")
        ).toDF.selectCSVcols("uid,uid_type,pFeature3")))
    )
    val expected = Seq("[42,OKID,3,2,true]")

    val res = EtlFeatures.makeDomainSource(cfg, sources).df.get
    val actual = res.collect.map(_.toString)

    res.schema.map(_.name) should contain theSameElementsInOrderAs "uid,uid_type,f3,f2,f1".splitTrim
    actual should contain theSameElementsAs expected
  }

}

object EtlFeaturesFunctionsTest {

  import StringToolbox._
//  import DefaultSeparators._
  import joiner.config._
  import joiner._
//  import joiner.implicits._

  def _show(df: DataFrame): Unit = {
    df.explain(extended = true)
    df.printSchema()
    df.show(numRows = 200, truncate = false)
  }

  object implicits {
    import scala.language.implicitConversions

    implicit def string2Source(names: String): DomainSourceDataFrame = {
      val items = names.splitTrim(Separators(","))

      DomainSourceDataFrame(
        domain = items.head,
        source = items(1),
        table = items(2),
        df = None)
    }

    implicit def string2NameWithAlias(name_alias: String): NameWithAlias = {
      name_alias.splitTrim(Separators(" as ")).toSeq match {
        case Seq(a) => NameWithAlias(a, a)
        //case Seq(a, b) => NameWithAlias(a, b)
        case Seq(a, b, _*) => NameWithAlias(a, b)
        case _ => sys.error(s"Malformed name_alias parameter `${name_alias}`")
      }
    }

  }

  val dataRow: Row = {
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

    new GenericRowWithSchema(Array[Any](
      Map("s42" -> "42"),
      Map("d42" -> 142.0, "dnull" -> null),
      null,
      Map("f42" -> 242.0f, "fnull" -> null),
      null,
      mutable.WrappedArray.make(Array(342.0, null)),
      null,
      mutable.WrappedArray.make(Array(442.0f, null)),
      null
    ), StructType(Seq(
      StructField("s_map_field", MapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("d_map_values", MapType(DataTypes.StringType, DataTypes.DoubleType)),
      StructField("d_map_nullfield", MapType(DataTypes.StringType, DataTypes.DoubleType)),
      StructField("f_map_values", MapType(DataTypes.StringType, DataTypes.FloatType)),
      StructField("f_map_nullfield", MapType(DataTypes.StringType, DataTypes.FloatType)),
      StructField("d_array_values", ArrayType(DataTypes.DoubleType)),
      StructField("d_array_nullfield", ArrayType(DataTypes.DoubleType)),
      StructField("f_array_values", ArrayType(DataTypes.FloatType)),
      StructField("f_array_nullfield", ArrayType(DataTypes.FloatType))
    )))
  }

  def inputDF(spark: SparkSession): DataFrame = {
    val row = dataRow
    val schema = row.schema

    spark.createDataset(Seq(row))(sql.Encoders.row(schema))
  }

  case class SourceRow
  (
    dt: Option[String] = None,
    uid: Option[Int] = None,
    uid_type: Option[String] = None,
    pFeature1: Option[Boolean] = None,
    pFeature2: Option[String] = None,
    pFeature3: Option[Int] = None,
    pFeature4: Option[Float] = None,
    pFeature5: Option[Double] = None,
    mFeature: Option[Map[String, Option[Int]]] = None,
    aFeature: Option[Array[Option[Int]]] = None,
    id: Option[Int] = None
  )

  object SourceRow {
    def apply(data: String): SourceRow = {
      import DefaultSeparators._
      val map = data.parseMap

      SourceRow(
        id = map.get("id").map(_.toInt),
        uid = map.get("uid").map(_.toInt),
        dt = map.get("dt"),
        uid_type = map.get("uid_type"),
        pFeature1 = map.get("pFeature1").map(_.toBoolean),
        pFeature2 = map.get("pFeature2"),
        pFeature3 = map.get("pFeature3").map(_.toDouble.toInt),
        pFeature4 = map.get("pFeature4").map(_.toFloat),
        pFeature5 = map.get("pFeature5").map(_.toDouble),
        aFeature = map.get("aFeature").map(arrayWithNulls(_, ";")),
        mFeature = map.get("mFeature").map(mapWithNulls(_, Separators(";", Some(Separators("=")))))
      )
    }

    def mapWithNulls(map: String, sep: StringToolbox.Separators): Map[String, Option[Int]] = {
      map.parseMap(Separators(";", Some(Separators("="))))
        .mapValues(v => Try(v.toDouble.toInt).toOption)
    }

    def arrayWithNulls(lst: String, sep: String): Array[Option[Int]] = {
      import DefaultSeparators._

      lst.splitTrimNoFilter(sep)
        .map(itm => Try(itm.toDouble.toInt).toOption)
        .toArray
    }

  }

  def dfFromCSV(text: String)(implicit spark: SparkSession): DataFrame = {
    import DefaultSeparators._
    import spark.implicits._

    val cleantext: Seq[String] = text.splitTrim("""\n""")
      .map(line =>
        line.splitTrim.mkString(",")
      )

    spark.read.option("header", "true").csv(cleantext.toDS)
  }

  def cols(df: DataFrame): Seq[(String, sql.types.DataType)] = df.schema.map(f => (f.name, f.dataType))

  private def printDiff(actual: String, expected: String): Unit = {
    val diffPos = expected.zipAll(actual, 'e', 'a').indexWhere(ea => ea._1 != ea._2)
    if (diffPos >= 0) {
      println(s"\nblock before diff: `${expected.substring(0, diffPos)}`<diff here>")
      println(s"\nexpected: `${expected.substring(diffPos, math.min(diffPos+90, expected.length))}` ...")
      println(s"\nactual: `${actual.substring(diffPos, math.min(diffPos+90, actual.length))}` ...\n")
    }
  }

  private def clean(txt: String): String = txt.replaceAll("""\n""", "").replaceAllLiterally(", ", ",")

}
