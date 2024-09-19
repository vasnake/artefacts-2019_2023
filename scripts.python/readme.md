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

- [Run python tests](./run-spark-local/run_tests.sh)
- [PySpark env setup snippets](./run-spark-local/setup-pyspark-env.sh)

- [Spark-submit wrapper, add timeout for yarn job 'accept' stage](./spark_submit_with_job_start_check/)

JVM procedure Python wrapper
- [InsertIntoHive python wrapper](luigi-pyspark-apps/spark_utils.py#insert_into_hive)
- [UDF/UDAF registration, register_all_udf](luigi-pyspark-apps/spark_utils.py#register_all_udf)

Spark.ml Python wrappers for JVM implementation of estimators and models (see spark.ml lib)
- [ScoreEqualizeTransformer + ScoreEqualizeTransformerModel](spark_ml/postprocessing.py#ScoreEqualizeTransformer)
- [NEPriorClassProbaTransformer + NEPriorClassProbaTransformerModel](spark_ml/postprocessing.py#NEPriorClassProbaTransformer)
- [ScoreQuantileThresholdTransformer + ScoreQuantileThresholdTransformerModel](spark_ml/postprocessing.py#ScoreQuantileThresholdTransformer)

ML model inference, first migration steps (to Scala-Apply)
- [Convert npz file](./simple-pyspark-apps/npz_to_json.py) (from numpy.savez_compressed) to json file
- [Simple spark job](./simple-pyspark-apps/score_audience.py), apply ML model to test data

Scala-Apply Python app and wrappers for JVM implementation
- [Luigi 'Apply' spark-submit task and 'apply_scala_models' method](luigi-pyspark-apps/scala_apply/apply_task.py#apply_scala_models)
- [Spark.ml transformer ApplyModelsTransformer](luigi-pyspark-apps/scala_apply/apply_models_transformer.py#ApplyModelsTransformer)
- [Three ML models adopted for Scala-Apply](luigi-pyspark-apps/scala_apply/ml_models_binary_rank.py)
- [Models json serialization code](luigi-pyspark-apps/scala_apply/sa_repr.py)
- [Tests for dynamic ML models serialization and Scala-Apply wrappers](./ml_models_repr/)

Join-Features Python app
- [Luigi 'JoinFeatures' spark-submit task](./luigi-pyspark-apps/join_features/app.py)
- ['JoinFeatures' beta version, saved for history](./luigi-pyspark-apps/join_features_beta/app.py)

Export (scored) records app
- [Luigi 'Export' spark-submit tasks](./luigi-pyspark-apps/export/README.md)

Combine ML features app
- [Luigi 'Combine' spark-submit task](./luigi-pyspark-apps/combine/README.md)

Experimental test library
- [Hive IO operations implemented on top of Spark](./luigi-pyspark-apps/combine/universal_features/test/_it/new_e2e/hive_io_spark.py#L120)

One-time scripts (adhoc)
- [A couple of adhoc scripts](./adhoc/)
- [Jupyter notebooks, experiments and adhoc scripts](./adhoc/notebook/)

TODO: all test scripts should run successfully (in proper docker container).
