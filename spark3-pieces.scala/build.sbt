import Dependencies._
import Dependencies.{ io => dio } // conflict with sbt.io
import MyUtil._

// Spark 3.5.2; Scala 2.12.19; sbt 1.10.1; java 1.8; Hadoop 3.3.4; Hive 2.3.9

ThisBuild / organization := "com.github.vasnake"
ThisBuild / scalaVersion := "2.12.19"
ThisBuild / fork := true // do we really need this in the global scope?
ThisBuild / compile / run / fork := true

ThisBuild / assembly / assemblyMergeStrategy := {
  case n if n.contains("holdenkarau") => MergeStrategy.discard // never needed
  case PathList("cats", xs @ _*)      => MergeStrategy.discard // provided
  case PathList("shapeless", xs @ _*) => MergeStrategy.discard // provided
  case x => (assembly / assemblyMergeStrategy).value(x)
}

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
        json,
        `etl-core`,
        `ml-core`,
        `ml-models`,
        `ml-models-json`,
        `spark-core`,
        `spark-udf`,
        // `spark-io`,
        // `spark-transformers`,
        // `spark-ml`,
        // `spark-apps`
      ).map(
        _ % Cctt
      ): _*
    )
    // Aggregation means that running a task on the aggregate project will also run it on the aggregated projects:
    .aggregate(
      core,
      common,
      text,
      json,
      `etl-core`,
      `ml-core`,
      `ml-models`,
      `ml-models-json`,
      `spark-core`,
      `spark-udf`,
      // `spark-io`,
      // `spark-transformers`,
      // `spark-ml`,
      // `spark-apps`
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
    .dependsOn(Seq(core).map(_ % Cctt): _*)
    .settings(commonSettings)
    .settings(commonDependencies)
    .settings(
      libraryDependencies ++= Seq(
        `commons-io`.`commons-io`,
        org.apache.commons.`commons-math3`
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
        org.unbescape.unbescape
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
        org.json4s.`json4s-ast`
      )
    )

lazy val `etl-core` =
  project
    .in(file("etl-core"))
    .dependsOn(Seq(core, common).map(_ % Cctt): _*)
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

lazy val `spark-core` =
  project
    .in(file("spark-core"))
    .dependsOn(Seq(core, common).map(_ % Cctt): _*)
    .settings(commonSettings)
    .settings(commonDependencies)
    .settings(sparkSettings)

lazy val `spark-udf` =
  project
    .in(file("spark-udf"))
    .dependsOn(Seq(core, common, text, `spark-core`).map(_ % Cctt): _*)
    .settings(commonSettings)
    .settings(commonDependencies)
    .settings(sparkSettings)

// lazy val `spark-io` =
//   project
//     .in(file("spark-io"))
//     .dependsOn(Seq(core, common, `spark-core`).map(_ % Cctt): _*)
//     .settings(commonSettings)
//     .settings(commonDependencies)
//     .settings(sparkSettings)

// lazy val `spark-transformers` =
//   project
//     .in(file("spark-transformers"))
//     .dependsOn(
//       Seq(core, common, text, `etl-core`, `ml-core`, `spark-core`, `spark-io`, json).map(
//         _ % Cctt
//       ): _*
//     )
//     .settings(commonSettings)
//     .settings(commonDependencies)
//     .settings(sparkSettings)

// lazy val `spark-ml` =
//   project
//     .in(file("spark-ml"))
//     .dependsOn(
//       Seq(`json`, `ml-core`, `ml-models`, `spark-core`, `spark-io`, `spark-transformers`).map(
//         _ % Cctt
//       ): _*
//     )
//     .settings(commonSettings)
//     .settings(commonDependencies)
//     .settings(sparkSettings)

// lazy val `spark-apps` =
//   project
//     .in(file("spark-apps"))
//     .dependsOn(
//       Seq(core, `spark-core`, `spark-io`, `spark-transformers`, `spark-ml`, `ml-models-json`, `hive-udaf-java`).map(
//         _ % Cctt
//       ): _*
//     )
//     .settings(commonSettings)
//     .settings(commonDependencies)
//     .settings(sparkSettings)
//     .settings(libraryDependencies ++= Seq(com.beust.jcommander))

// settings

lazy val sparkSettings = {

  lazy val dependencies = Seq(
    libraryDependencies ++= (org.apache.spark.sparkModules ++ Seq(
      org.json4s.`json4s-jackson`,
      org.json4s.`json4s-ast`
    )).map(_ % Provided),
    libraryDependencies ++= Seq(com.holdenkarau.`spark-testing-base`)
      .map(_ % Test),

    dependencyOverrides ++= Seq(
      // spark 3.5.2 requirements, override deps. from json module:
      "org.json4s" %% "json4s-jackson" % "3.7.0-M11",
      "org.json4s" %% "json4s-ast" % "3.7.0-M11",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2"
    ).map(_ % Test),
  )

  lazy val options = Seq(
    // https://github.com/holdenk/spark-testing-base
    javaOptions ++= Seq("-Xms4G", "-Xmx4G", "-XX:+CMSClassUnloadingEnabled"),
    fork := true, // this is working, but this not: Test / fork := true, // fork in Test := true, // show test / fork
    Test / parallelExecution := false, // parallelExecution in Test := false,
  )

  dependencies ++ options
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
  )

  lazy val consoleScalacOptions = Seq(
    Compile / console / scalacOptions := {
      (Compile / console / scalacOptions)
        .value
        .filterNot(_.contains("wartremover"))
        .filterNot(Scalac.Lint.toSet)
        .filterNot(Scalac.FatalWarnings.toSet) :+ "-Wconf:any:silent"
    },
    Test / console / scalacOptions :=
      (Compile / console / scalacOptions).value
  )

  lazy val otherSettings = Seq(
    update / evictionWarningOptions := EvictionWarningOptions.empty,

    // sbt> print scalacOptions
    scalacOptions ++= Seq(
      // This will elide WARNING level end below (WARNING, INFO, CONFIG, FINE, ...):
      "-Xelide-below",
      sys.props.getOrElse("elide.below", "901")
      // To elide ASSERTION level and below (ASSERTION, SEVERE, WARNING, ...) run: `sbt -Delide.below=2001 ...`
      // `elidable`: An annotation for methods whose bodies may be excluded from compiler-generated bytecode
    )
  )

  lazy val assemblySettings = Seq(
    assembly / test := {},
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeDependency(true).withIncludeScala(false)
  )

  Seq(
    compilerPlugins,
    consoleScalacOptions,
    otherSettings,
    assemblySettings
  ).reduceLeft(_ ++ _)
}

// dependencies

lazy val commonDependencies = Seq(
  libraryDependencies ++= Seq(
    com.`storm-enroute`.scalameter,
    org.scalatest.scalatest
  ).map(_ % Test),
)
