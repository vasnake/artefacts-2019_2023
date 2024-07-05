import Dependencies._
import MyUtil._

ThisBuild / organization := "com.github.vasnake"
ThisBuild / scalaVersion := "2.12.19"

lazy val `etl-ml-pieces-1923` =
  project
    .in(file("."))
    .dependsOn(macros % Cctt)
    .settings(name := "etl-ml-pieces-1923")
    .settings(commonSettings)
    .settings(dependencies)
    //    .settings(autoImportSettings)
    .aggregate(macros)

lazy val macros =
  project
    .in(file("macros"))
    .settings(commonSettings)
    .settings(dependencies)

lazy val commonSettings = {
  lazy val commonCompilerPlugins = Seq(
    addCompilerPlugin(com.olegpy.`better-monadic-for`),
    addCompilerPlugin(org.augustjune.`context-applied`),
    addCompilerPlugin(org.typelevel.`kind-projector`),
  )

  lazy val commonScalacOptions = Seq(
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

  lazy val otherCommonSettings = Seq(
    update / evictionWarningOptions := EvictionWarningOptions.empty,
    scalacOptions += s"-Wconf:src=${target.value}/.*:s",
  )

  Seq(
    commonCompilerPlugins,
    commonScalacOptions,
    otherCommonSettings,
  ).reduceLeft(_ ++ _)
}

lazy val dependencies = Seq(
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

// -Yimports IMPORT1,IMPORT2 (https://docs.scala-lang.org/overviews/compiler-options/index.html)
//lazy val autoImportSettings = Seq(
//  scalacOptions +=
//    Seq(
//      "java.lang",
//      "scala",
//      "scala.Predef",
//      "scala.annotation",
//      "scala.util.chaining",
//    ).mkString(start = "-Yimports ", sep = ",", end = ""),
//  Test / scalacOptions +=
//    Seq(
//      "derevo",
//      "derevo.scalacheck",
//      "org.scalacheck",
//      "org.scalacheck.Prop",
//    ).mkString(start = "-Yimports ", sep = ",", end = ""),
//)
