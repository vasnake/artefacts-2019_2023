import Dependencies._
import Dependencies.{ io => dio } // conflict with sbt.io
import MyUtil._

// Spark 2.4.8; Scala 2.12.19: it was production setup for our team

ThisBuild / organization := "com.github.vasnake"
ThisBuild / scalaVersion := "2.12.19"

// project

lazy val `etl-ml-pieces-1923` =
  project
    .in(file("."))
    // To compile and test this project you need this dependencies:
    .dependsOn(
      Seq(
        core,
        common,
        text,
        `etl-core`,
        `ml-core`,
        `ml-models`,
        json,
        `ml-models-json`,
        `hive-udaf-java`,
        `spark-udf`,
        `spark-io`,
        `spark-transformers`,
      ).map(
        _ % Cctt
      ): _*
    )
    // Aggregation means that running a task on the aggregate project will also run it on the aggregated projects:
    .aggregate(
      core,
      common,
      text,
      `etl-core`,
      `ml-core`,
      `ml-models`,
      json,
      `ml-models-json`,
      `hive-udaf-java`,
      `spark-udf`,
      `spark-io`,
      `spark-transformers`,
    )
    .settings(name := "etl-ml-pieces-1923")
    .settings(commonSettings)
    .settings(commonDependencies)

// modules

lazy val core =
  project
    .in(file("core"))
    .settings(commonSettings)
    .settings(commonDependencies)

lazy val common =
  project
    .in(file("common"))
    .settings(commonSettings)
    .settings(commonDependencies)
    .settings(
      libraryDependencies ++= Seq(
        `commons-io`.`commons-io`,
        org.apache.commons.`commons-math3`,
      )
    )

lazy val text =
  project
    .in(file("text"))
    .settings(commonSettings)
    .settings(commonDependencies)
    .settings(
      libraryDependencies ++= Seq(
        org.`scala-lang`.modules.`scala-parser-combinators`,
        org.unbescape.unbescape,
      )
    )

lazy val json =
  project
    .in(file("json"))
    .settings(commonSettings)
    .settings(commonDependencies)
    .settings(
      libraryDependencies ++= Seq(
        dio.circe.`circe-core`,
        dio.circe.`circe-generic`,
        dio.circe.`circe-parser`,
        org.json4s.`json4s-jackson`,
      )
    )

lazy val `etl-core` =
  project
    .in(file("etl-core"))
    .dependsOn(Seq(core).map(_ % Cctt): _*)
    .settings(commonSettings)
    .settings(commonDependencies)

lazy val `ml-core` =
  project
    .in(file("ml-core"))
    .dependsOn(Seq(core, common).map(_ % Cctt): _*)
    .settings(commonSettings)
    .settings(commonDependencies)
    .settings(libraryDependencies ++= Seq(org.pmml4s.pmml4s))

lazy val `ml-models` =
  project
    .in(file("ml-models"))
    .dependsOn(Seq(`etl-core`, `ml-core`).map(_ % Cctt): _*)
    .settings(commonSettings)
    .settings(commonDependencies)

lazy val `ml-models-json` =
  project
    .in(file("ml-models-json"))
    .dependsOn(Seq(json, `ml-models`).map(_ % Cctt): _*)
    .settings(commonSettings)
    .settings(commonDependencies)

lazy val `hive-udaf-java` =
  project
    .in(file("hive-udaf-java"))
    .settings(commonSettings)
    .settings(commonDependencies)
    .settings(hiveSettings)

lazy val `spark-udf` =
  project
    .in(file("spark-udf"))
    .dependsOn(Seq(core, common, text).map(_ % Cctt): _*)
    .settings(commonSettings)
    .settings(commonDependencies)
    .settings(sparkSettings)

lazy val `spark-io` =
  project
    .in(file("spark-io"))
    .dependsOn(Seq(core, common).map(_ % Cctt): _*)
    .settings(commonSettings)
    .settings(commonDependencies)
    .settings(sparkSettings)

lazy val `spark-transformers` =
  project
    .in(file("spark-transformers"))
    .dependsOn(Seq(core, common, text, `etl-core`).map(_ % Cctt): _*)
    .settings(commonSettings)
    .settings(commonDependencies)
    .settings(sparkSettings)

// settings

lazy val sparkSettings = {

  lazy val dependencies = Seq(
    libraryDependencies ++= org.apache.spark.sparkModules.map(_ % Provided)
  )

  dependencies
}

lazy val hiveSettings = {

  lazy val dependencies = Seq(
    libraryDependencies ++= Seq(org.apache.hive.`hive-exec`).map(_ % Provided)
  )

  lazy val options = Seq(
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:deprecation"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled"),
  )

  // libraryDependencies += "org.pentaho" % "pentaho-aggdesigner-algorithm" % "5.1.5-jhyde" % Test,
  lazy val repos = Seq(
    resolvers ++= Seq(
      Resolver.mavenLocal,
      "huawei-maven" at "https://repo.huaweicloud.com/repository/maven/huaweicloudsdk/",
    )
  )
// Note: problem with `Error downloading org.pentaho:pentaho-aggdesigner-algorithm`. Following not working:
// > Note: this artifact is located at Spring Plugins repository (https://repo.spring.io/plugins-release/)
// > https://mvnrepository.com/artifact/org.pentaho/pentaho-aggdesigner-algorithm
// > libraryDependencies += "org.pentaho" % "pentaho-aggdesigner-algorithm" % "5.1.5-jhyde" % Test
//      resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
//      resolvers += "Cascading repo" at "https://conjars.org/repo",
//      resolvers += "Spring Plugins" at "https://repo.spring.io/plugins-release/",
//      resolvers += "Nexus Pentaho" at "https://public.nexus.pentaho.org/repository/proxy-public-3rd-party-release",

  dependencies ++ options ++ repos
}

lazy val commonSettings = {

  lazy val compilerPlugins = Seq(
    addCompilerPlugin(org.typelevel.`kind-projector`)
    // addCompilerPlugin(com.olegpy.`better-monadic-for`),
    // addCompilerPlugin(org.augustjune.`context-applied`),
  )

  lazy val consoleScalacOptions = Seq(
    // tasks: console          Starts the Scala interpreter with the project classes on the classpath
    Compile / console / scalacOptions := {
      (Compile / console / scalacOptions)
        .value
        .filterNot(_.contains("wartremover"))
        .filterNot(Scalac.Lint.toSet)
        .filterNot(Scalac.FatalWarnings.toSet) :+ "-Wconf:any:silent"
    },
    Test / console / scalacOptions :=
      (Compile / console / scalacOptions).value,
  )

  lazy val otherSettings = Seq(
    // tasks: update           Resolves and optionally retrieves dependencies, producing a report
    update / evictionWarningOptions := EvictionWarningOptions.empty,

    // sbt> print scalacOptions
    scalacOptions ++= Seq(
      // This will elide WARNING level end below (WARNING, INFO, CONFIG, FINE, ...):
      "-Xelide-below",
      sys.props.getOrElse("elide.below", "901"),
      // To elide ASSERTION level and below (ASSERTION, SEVERE, WARNING, ...) run: `sbt -Delide.below=2001 ...`
      // `elidable`: An annotation for methods whose bodies may be excluded from compiler-generated bytecode
    ),
  )

  Seq(
    compilerPlugins,
    consoleScalacOptions,
    otherSettings,
  ).reduceLeft(_ ++ _)
}

// dependencies

lazy val commonDependencies = Seq(
  libraryDependencies ++= Seq(
    // main dependencies
  ),
  libraryDependencies ++= Seq(
    com.eed3si9n.expecty.expecty,
    org.scalacheck.scalacheck,
    org.scalameta.`munit-scalacheck`,
    org.scalameta.munit,
    org.typelevel.`discipline-munit`,
    tf.tofu.`derevo-scalacheck`,
  ).map(_ % Test),
)
