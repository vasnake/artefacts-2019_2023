# Scripts and modules (Python)

WIP

Collection of some interesting pieces from my projects.

## What do we have here

- [Run local Spark cluster](./run-spark-local/run-spark-standalone.sh)
- [Local spark-submit, scala-apply benchmarks](./run-spark-local/spark-submit-scala-apply-test.sh)
- [Insert-into-hive procedure tests, runner](./run-spark-local/spark-submit-writer-test.sh)
- [Insert-into-hive procedure tests, script](./run-spark-local/writer_test.py)
- [Unfinished experiments, spark-submit app logger](./spark-submit-app-logger/readme.md)

- [Hive UDAF tests, runner](./run-spark-local/spark-submit-hive-udaf-test.sh)
- [Hive UDAF tests, script](./run-spark-local/hive_udaf_test.py)

Тесты быстродействия scala-apply трансформера, задача https://jira.mail.ru/browse/DM-6915

???

- npz_to_json.py
Скрипт конвертации моделей (score_audience) из numpy npz в json, для использования в scala-apply.

- integration tests scripts (sh, py)
- wrappers for spark-scala classes, functions
- apps: learn, apply, export, join
