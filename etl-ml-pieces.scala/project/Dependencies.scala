import sbt._

object Dependencies {
  object io {
    object circe {
      // https://mvnrepository.com/artifact/io.circe/circe-core
      // libraryDependencies += "io.circe" %% "circe-core" % "0.14.9"
      val `circe-core` = "io.circe" %% "circe-core" % "0.12.0-M3"
      val `circe-generic` = "io.circe" %% "circe-generic" % "0.12.0-M3"
      val `circe-parser` = "io.circe" %% "circe-parser" % "0.12.0-M3"
    }
  }

  object com {
    object `storm-enroute` {
      val scalameter = "com.storm-enroute" %% "scalameter" % "0.19"
    }

    object holdenkarau {
      // libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.4.8_1.5.3" % Test
      val `spark-testing-base` =
        "com.holdenkarau" %% "spark-testing-base" % s"${org.apache.spark.sparkVersion}_1.5.3" // % Test
    }

    object beust {
      val jcommander = "com.beust" % "jcommander" % "1.82"
    }

    object eed3si9n {
      object expecty {
        val expecty =
          "com.eed3si9n.expecty" %% "expecty" % "0.16.0"
      }
    }

    object olegpy {
      val `better-monadic-for` =
        "com.olegpy" %% "better-monadic-for" % "0.3.1"
    }
  }

  object `commons-io` {
    // https://mvnrepository.com/artifact/commons-io/commons-io
    //  libraryDependencies += "commons-io" % "commons-io" % "2.16.1"
    val `commons-io` =
      "commons-io" % "commons-io" % "2.16.1"
  }

  object org {
    object scalatest {
      val scalatest = "org.scalatest" %% "scalatest" % "3.2.19"
    }

    object unbescape {
      // https://mvnrepository.com/artifact/org.unbescape/unbescape
      val unbescape = "org.unbescape" % "unbescape" % "1.1.6.RELEASE"
    }

    object json4s {
      // > Spark depends on old version json4s. You should use old json4s or another json library
      // https://github.com/apache/spark/blob/v2.4.8/dev/deps/spark-deps-hadoop-2.7 => 3.5.3

      // https://mvnrepository.com/artifact/org.json4s/json4s-jackson
      // libraryDependencies += "org.json4s" %% "json4s-jackson" % "4.0.7"
      val `json4s-jackson` = "org.json4s" %% "json4s-jackson" % "3.5.3"

      // https://mvnrepository.com/artifact/org.json4s/json4s-ast
      // libraryDependencies += "org.json4s" %% "json4s-ast" % "4.0.7"
      val `json4s-ast` = "org.json4s" %% "json4s-ast" % "3.5.3"
      // other (than 3.5.3 for spark 2.4.8) versions lead to errors like this:
      // 24/08/09 11:46:23 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
      // [info] com.github.vasnake.spark.features.vector.FeaturesRowDecoderTest *** ABORTED ***
      // [info]   java.lang.NoClassDefFoundError: org/json4s/JsonAST$JValue
      // Solution: one old version for all modules; or
      // you could shadow unwanted versions; or
      // you could use 'dependencyOverrides' option (current solution)
    }

    object pmml4s {
      // https://mvnrepository.com/artifact/org.pmml4s/pmml4s
      // libraryDependencies += "org.pmml4s" %% "pmml4s" % "1.0.1"
      val pmml4s = "org.pmml4s" %% "pmml4s" % "1.0.1"
    }

    object apache {
      object spark {
        val sparkVersion = "2.4.8"
        val sparkModules = Seq(
          "org.apache.spark" %% "spark-hive",
          "org.apache.spark" %% "spark-core",
          "org.apache.spark" %% "spark-sql",
          "org.apache.spark" %% "spark-mllib",
        ).map(_ % sparkVersion)
      }

      object hive {
        // https://mvnrepository.com/artifact/org.apache.hive/hive-exec
        // libraryDependencies += "org.apache.hive" % "hive-exec" % "2.1.1"
        val `hive-exec` = "org.apache.hive" % "hive-exec" % "2.1.1"
      }
      object commons {
        // https://mvnrepository.com/artifact/org.apache.commons/commons-math3
        // libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
        val `commons-math3` =
          "org.apache.commons" % "commons-math3" % "3.6.1"
      }
    }

    object `scala-lang` {
      object modules {
        // https://mvnrepository.com/artifact/org.scala-lang.modules/scala-parser-combinators
        // libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0"
        val `scala-parser-combinators` = "org.scala-lang.modules" %% "scala-parser-combinators" % "2.2.0"
      }
    }

    object augustjune {
      val `context-applied` =
        "org.augustjune" %% "context-applied" % "0.1.4"
    }

    object scalacheck {
      val scalacheck =
        "org.scalacheck" %% "scalacheck" % "1.18.0"
    }

    object scalameta {
      val munit =
        moduleId("munit")

      val `munit-scalacheck` =
        moduleId("munit-scalacheck")

      private def moduleId(artifact: String): ModuleID =
        "org.scalameta" %% artifact % "1.0.0"
    }

    object typelevel {
      val `discipline-munit` =
        "org.typelevel" %% "discipline-munit" % "2.0.0"

      val `kind-projector` =
        "org.typelevel" %% "kind-projector" % "0.13.3" cross CrossVersion.full
    }
  }

  object tf {
    object tofu {
      val `derevo-scalacheck` =
        "tf.tofu" %% "derevo-scalacheck" % "0.13.0"
    }
  }
}
