import Scalac.Keys._

// https://docs.scala-lang.org/overviews/compiler-options/index.html

ThisBuild / scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-deprecation", // Emit warning and location for usages of deprecated APIs
  "-unchecked", // Enable additional warnings where generated code depends on assumptions
  "-feature", // Emit warning and location for usages of features that should be imported explicitly
  "-language:higherKinds", // Enable or disable language features
  // "-release", "8", // The -release option specifies the target version, such as “8” or “18”.
  // "-target", "1.8", // deprecated option -target does not compile against the desired API, but only specifies a target class file format

  // Y: private
  "-Ydelambdafy:inline",

  // W: warning
  // "-Wunused:imports", // always on for OrganizeImports // Warn if an import selector is not referenced
  // A Scala compiler option is required to use OrganizeImports with "OrganizeImports.removeUnused" set to true
  "-Ywarn-unused-import",

  // Configure compiler warnings. // Syntax: -Wconf:<filters>:<action>,<filters>:<action>
  "-Wconf:any:warning-verbose",
) ++ warnings.value ++ lint.value

ThisBuild / warnings := {
  if (insideCI.value)
    Seq(
      "-Wconf:any:error",
      "-Xfatal-warnings", // for wartremover warts // -Werror or -Xfatal-warnings : Fail the compilation if there are any warnings.
    )
  else if (lintOn.value)
    Seq(
      "-Wconf:any:warning-verbose"
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

ThisBuild / wartremoverWarnings := {
  if (shouldLint.value)
    Seq.empty
  else
    (ThisBuild / wartremoverWarnings).value
}
