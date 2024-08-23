# Scripts and modules (Python)

Collection of some interesting pieces from my projects.

## What do we have here

Integration test, bash scripts
- [Unfinished experiments, spark-submit app logger](./spark-submit-app-logger/readme.md)

- [Run local Spark cluster](./run-spark-local/run-spark-standalone.sh)
- [Local spark-submit, scala-apply benchmarks](./run-spark-local/spark-submit-scala-apply-test.sh)

- [Insert-into-hive procedure tests, runner](./run-spark-local/spark-submit-writer-test.sh)
- [Insert-into-hive procedure tests, script](./run-spark-local/writer_test.py)

- [Hive UDAF tests, runner](./run-spark-local/spark-submit-hive-udaf-test.sh)
- [Hive UDAF tests, script](./run-spark-local/hive_udaf_test.py)

- [Spark Catalyst UDF tests, runner](./run-spark-local/spark-submit-catalyst-udf-test.sh)
- [Spark Catalyst UDF tests, script](./run-spark-local/catalyst_udf_test.py)
- [Spark Catalyst UDF tests, ipynb](./run-spark-local/catalyst_udf_test.ipynb)

- [Spark Java UDF tests, runner](./run-spark-local/spark-submit-java-udf-test.sh)
- [Spark Java UDF tests, script](./run-spark-local/java_udf_test.py)

JVM procedure wrapper
- [InsertIntoHive python wrapper](luigi-pyspark-apps/spark_utils.py#insert_into_hive)
- [UDF/UDAF registration, register_all_udf](luigi-pyspark-apps/spark_utils.py#register_all_udf)

Scala-Apply wrappers and helpers for JVM implementation of the ScalaApply project
- [Luigi 'Apply' task and 'apply_scala_models' method](luigi-pyspark-apps/scala_apply/apply_task.py#apply_scala_models)
- [Spark.ml transformer ApplyModelsTransformer](luigi-pyspark-apps/scala_apply/apply_models_transformer.py#ApplyModelsTransformer)
- [Three ML models adopted for Scala-Apply](luigi-pyspark-apps/scala_apply/ml_models_binary_rank.py)
- [Models json serialization code](luigi-pyspark-apps/scala_apply/sa_repr.py)

Spark.ml wrappers for JVM implementation of estimators and models (see spark.ml lib)
- [ScoreEqualizeTransformer + ScoreEqualizeTransformerModel](spark_ml/postprocessing.py#ScoreEqualizeTransformer)
- [NEPriorClassProbaTransformer + NEPriorClassProbaTransformerModel](spark_ml/postprocessing.py#NEPriorClassProbaTransformer)
- [ScoreQuantileThresholdTransformer + ScoreQuantileThresholdTransformerModel](spark_ml/postprocessing.py#ScoreQuantileThresholdTransformer)

???

- npz_to_json.py (transformers\python_sandbox\README.md)
Скрипт конвертации моделей (score_audience) из numpy npz в json, для использования в scala-apply.

- integration tests scripts (sh, py)
- wrappers for spark-scala classes, functions
- apps: learn, apply, export, join

TODO: all scripts should run successfully, in proper docker container.
