import Dependencies._
import MyUtil._

// Spark 2.4.8; Scala 2.12.19: it was production setup for our team

ThisBuild / organization := "com.github.vasnake"
ThisBuild / scalaVersion := "2.12.19"

// project

lazy val `etl-ml-pieces-1923` =
  project
    .in(file("."))
    // To compile and test this project you need this dependencies:
    .dependsOn(Seq(core, common, text, `etl-core`).map(_ % Cctt): _*)
    // Aggregation means that running a task on the aggregate project will also run it on the aggregated projects:
    .aggregate(core, common, text, `etl-core`)
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
    .settings(libraryDependencies ++= Seq(org.`scala-lang`.modules.`scala-parser-combinators`))

lazy val `etl-core` =
  project
    .in(file("etl-core"))
    .dependsOn(Seq(core).map(_ % Cctt): _*)
    .settings(commonSettings)
    .settings(commonDependencies)

// settings

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
