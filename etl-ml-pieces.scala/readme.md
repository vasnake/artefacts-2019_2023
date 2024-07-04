# Spark/Scala modules (mainly)

Collection of some interesting pieces from my projects.

Spark 2.4.8; Scala 2.12.19; sbt 1.10.0

My local station (win11 + wsl2) env
```sh
# run project sbt
alias psbt='export JAVA_OPTS="-XX:MaxMetaspaceSize=1G -Xmx4G -XX:+UseParallelGC" && pushd /mnt/c/Users/valik/data/github/artefacts-2019_2023/etl-ml-pieces.scala/ && sbt -v && popd'

# etl-ml-pieces-1923
pushd /mnt/c/Users/valik/data/github/artefacts-2019_2023/etl-ml-pieces.scala/
sbt new devinsideyou/scala-seed.g8
# produced project with this parameters:
    name [etl-ml-pieces-1923]:
    organization [com.github.vasnake]:
    package [interesting.pieces.1923]:
# some tuning required
```
WSL shit
