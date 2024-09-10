import MyUtil._

addCommandAlias("l", "projects")
addCommandAlias("ll", "projects")
addCommandAlias("ls", "projects")
addCommandAlias("cd", "project")
addCommandAlias("root", "cd etl-ml-pieces-1923")
addCommandAlias("c", "compile")
addCommandAlias("t", "testQuick")
addCommandAlias("tt", "test")
addCommandAlias("a", "assembly")
addCommandAlias("r", "reload")
addCommandAlias(
  "sc", // fmt, fix checks
  "scalafmtSbtCheck; scalafmtCheckAll; Test / compile; scalafixAll --check",
)
addCommandAlias(
  "sf", // fix, fmt
  "Test / compile; scalafixAll; scalafmtSbt; scalafmtAll",
)
addCommandAlias(
  "up2date",
  "reload plugins; dependencyUpdates; reload return; dependencyUpdates",
)

onLoadMessage +=
  s"""|
      |╭─────────────────────────────────╮
      |│     List of defined ${styled("aliases")}     │
      |├─────────────┬───────────────────┤
      |│ ${styled("l")} | ${styled("ll")} | ${styled("ls")} │ projects          │
      |│ ${styled("cd")}          │ project           │
      |│ ${styled("root")}        │ cd root           │
      |│ ${styled("c")}           │ compile           │
      |│ ${styled("t")}           │ testQuick         │
      |│ ${styled("tt")}          │ test              │
      |│ ${styled("a")}           │ assembly          │
      |│ ${styled("r")}           │ reload            │
      |│ ${styled("sc")}          │ sfmt, sfix checks │
      |│ ${styled("sf")}          │ sfix, sfmt        │
      |│ ${styled("up2date")}     │ dependencyUpdates │
      |╰─────────────┴───────────────────╯""".stripMargin
// print onLoadMessage
