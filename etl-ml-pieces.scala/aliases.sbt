import MyUtil._

addCommandAlias("l", "projects")
addCommandAlias("ll", "projects")
addCommandAlias("ls", "projects")
addCommandAlias("cd", "project")
addCommandAlias("root", "cd etl-ml-pieces-1923")
addCommandAlias("c", "compile")
addCommandAlias("t", "test")
addCommandAlias("r", "reload")
addCommandAlias(
  "styleCheck",
  "scalafmtSbtCheck; scalafmtCheckAll; Test / compile; scalafixAll --check",
)
addCommandAlias(
  "styleFix",
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
      |│ ${styled("t")}           │ test              │
      |│ ${styled("r")}           │ reload            │
      |│ ${styled("styleCheck")}  │ fmt & fix checks  │
      |│ ${styled("styleFix")}    │ fix then fmt      │
      |│ ${styled("up2date")}     │ dependencyUpdates │
      |╰─────────────┴───────────────────╯""".stripMargin
// print onLoadMessage
