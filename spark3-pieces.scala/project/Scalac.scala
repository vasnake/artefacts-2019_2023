import sbt._

object Scalac {
  val Lint: Seq[String] = Seq(
    "-Ywarn-dead-code" //      "-Wdead-code",
  )

  val FatalWarnings: Seq[String] = Seq(
    "-Xfatal-warnings"
  )

  object Keys {
    val lint =
      settingKey[Seq[String]]("Decides weather to use lint related options.")

    val warnings =
      settingKey[Seq[String]]("Configures warnings.")

    val lintOn =
      settingKey[Boolean]("""By default: !sys.env.contains("LINT_OFF")""")

    val shouldLint =
      settingKey[Boolean]("By default: true unless lint is off and not in CI.")
  }
}
