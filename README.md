# artefacts-2019_2023

Collection of some interesting pieces from my projects (2019 .. 2023). Spark, Scala, Python, sh

Two main categories:
- [Spark/Scala stuff](./etl-ml-pieces.scala/readme.md)
- [Python scripts, modules](./scripts.python/readme.md)

For full description see docs inside.

CI/CD
- [Build fat-jar for Spark](cicd/build_uber_jar.sh)

Cleanup: `find . -depth -type d \( -name target -or -name .bloop -or -name .bsp -or -name .metals -or -name metastore_db \) -exec echo rm -rf {} \;`
