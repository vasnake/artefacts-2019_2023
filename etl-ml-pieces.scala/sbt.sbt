import MyUtil._

Global / onChangedBuildSource := ReloadOnSourceChanges

Global / excludeLintKeys ++= Set(
  autoStartServer, // When set to true, sbt shell will automatically start sbt server.
  // Otherwise, it will not start the server until startSever command is issued

  turbo, // Turbo mode with ClassLoader layering
  evictionWarningOptions,
)

// -o[configs...] - causes test results to be written back to sbt, which usually displays it on the standard output
// D - show all durations
// S - show short stack traces
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oSD")
Test / parallelExecution := false
Test / turbo := true

//ThisBuild / autoStartServer := insideCI.value
ThisBuild / includePluginResolvers := true
ThisBuild / turbo := true

ThisBuild / watchBeforeCommand := Watch.clearScreen
ThisBuild / watchTriggeredMessage := Watch.clearScreenOnTrigger
ThisBuild / watchForceTriggerOnAnyChange := true

ThisBuild / shellPrompt := { state => s"${prompt(projectName(state))}> " }

ThisBuild / watchStartMessage := {
  case (iteration, ProjectRef(build, projectName), commands) =>
    Some {
      s"""|~${commands.map(styled).mkString(";")}
          |Monitoring source files for ${prompt(projectName)}...""".stripMargin
    }
}
