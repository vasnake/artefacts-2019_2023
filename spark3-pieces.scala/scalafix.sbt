import Dependencies._

ThisBuild / semanticdbEnabled := true // produce semanticdb files to run semantic rules like RemoveUnused
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision // print semanticdbVersion
