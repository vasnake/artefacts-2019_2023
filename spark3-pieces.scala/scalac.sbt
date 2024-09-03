import Scalac.Keys._

// https://docs.scala-lang.org/overviews/compiler-options/index.html

ThisBuild / scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-deprecation", // Emit warning and location for usages of deprecated APIs
  "-unchecked", // Enable additional warnings where generated code depends on assumptions
  "-feature", // Emit warning and location for usages of features that should be imported explicitly
  "-language:higherKinds", // Enable or disable language features

  // Y: private
  "-Ydelambdafy:inline",
  // W: warning
  "-Ywarn-unused-import",
) ++ warnings.value ++ lint.value

ThisBuild / warnings := {
  if (insideCI.value)
    Seq(
      "-Wconf:any:error",
      "-Xfatal-warnings", // for wartremover warts // -Werror or -Xfatal-warnings : Fail the compilation if there are any warnings.
    )
  else if (lintOn.value)
    Seq(
      // "-Wconf:any:warning-verbose",
    )
  else
    Seq(
      "-Wconf:any:warning"
    )
}

ThisBuild / lint := {
  if (shouldLint.value)
    Scalac.Lint
  else
    Seq.empty
}

ThisBuild / shouldLint := insideCI.value || lintOn.value // true if LINT_OFF not set

ThisBuild / lintOn := !sys.env.contains("LINT_OFF")
