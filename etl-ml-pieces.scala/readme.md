# Spark/Scala modules

Collection of some interesting bits and pieces from my projects.

Spark 2.4.8; Scala 2.12.19; sbt 1.10.1; java 1.8

[Spark 3 version](../spark3-pieces.scala/readme.md).

After migrating this collection to Spark3 platform,
I don't need (and don't want) Spark2-related code.
Consider it deprecated, it sits here just for educational purposes.

## Project modules

All modules could be packed to uber-jar (via `sbt assembly`) and can be used in spark apps.
To do that, you should add library to spark session, e.g: `spark-submit ... --jars hdfs:/lib/fat.jar`.

### spark-udf

Spark extension, generic UDFs and UDAFs using Spark Catalyst, accessible from Spark SQL and PySpark.
Those custom functions work just like Spark native (builtin) SQL functions.

Before using mentioned here UDF/UDAF in your spark session you have to register them: `com.github.vasnake.spark.udf.catalog.registerAll(spark)`

The set of generic SQL UDAF functions: `gavg, gsum, gmin, gmax, most_freq`.
Supported data types: primitive numeric types, arrays of primitive numeric types, and maps
with keys: `float`, `double`, `int`, `byte`, `long`, `short`, `bool`, `date`, `timestamp`, `string`;
and values: `float`, `double`, `int`, `byte`, `long`, `short`, `decimal`
- org.apache.spark.sql.catalyst.vasnake.udf.GenericMin
- org.apache.spark.sql.catalyst.vasnake.udf.GenericMax
- org.apache.spark.sql.catalyst.vasnake.udf.GenericSum
- org.apache.spark.sql.catalyst.vasnake.udf.GenericAvg
- org.apache.spark.sql.catalyst.vasnake.udf.GenericMostFreq

The set of generic vector/matrix UDF:
- org.apache.spark.sql.catalyst.vasnake.udf.GenericVectorCooMul
- org.apache.spark.sql.catalyst.vasnake.udf.GenericVectorSemiSum
- org.apache.spark.sql.catalyst.vasnake.udf.GenericVectorSemiDiff
- org.apache.spark.sql.catalyst.vasnake.udf.GenericVectorMatMul

Two generic functions, complementary to builtin `isnan`:
- org.apache.spark.sql.catalyst.vasnake.udf.GenericIsInf
- org.apache.spark.sql.catalyst.vasnake.udf.GenericIsFinite

The set of non-generic trivial UDF:
- com.github.vasnake.spark.udf.`java-api`.HtmlUnescapeUDF
- com.github.vasnake.spark.udf.`java-api`.MapValuesOrderedUDF
- com.github.vasnake.spark.udf.`java-api`.CheckUINT32UDF
- com.github.vasnake.spark.udf.`java-api`.HashToUINT32UDF
- com.github.vasnake.spark.udf.`java-api`.MurmurHash3_32UDF
- com.github.vasnake.spark.udf.`java-api`.MapJoinUDF
- com.github.vasnake.spark.udf.`java-api`.Uid2UserUDF

### hive-udaf-java

Class `com.github.vasnake.hive.java.udaf.GenericAvgUDAF`: generic UDAF based on the old Hive API `hive.ql.udf.generic`.
Can be used on columns of type `array<numeric>`, `map<string, numeric>`, along with plain numeric types.

I don't recommend it, you should use Spark Catalyst API for UDF/UDAF development.

### spark-io

Method `com.github.vasnake.spark.io.hive.TableSmartWriter.insertIntoHive`: insert partitions into Hive (partitioned) table or,
if table is not partitioned, overwrite table.
Uses `df.write.insertInto(tableFQN)` under the hood.

Method has two distinct features:
- Resulting files size are even, and under control of `maxRowsPerBucket` parameter;
- In HMS, the boolean flag maintained for each written partition. It's semantics similar to `SUCCESS_` flag for HDFS directory.

The second feature use custom ExternalCatalog implementation combined with the parallel-query-processor
based on the managed pool of HMS (Hive Meta Store) query connections.
This code was written to solve the problem with Spark ExternalCatalog inability to process queries concurrently, in the context of a single session.
- Custom external catalog: `org.apache.spark.sql.hive.vasnake.HiveExternalCatalog`.
- HMS query processor: `org.apache.spark.sql.hive.vasnake.MetastoreQueryProcessorWithConnPool`.

Other spark-io modules:
* com.github.vasnake.spark.io.HDFSFileToolbox
* com.github.vasnake.spark.io.CheckpointService
* com.github.vasnake.spark.io.hive.SQLHiveWriter
* com.github.vasnake.spark.io.hive.TableSmartReader.readTableAsUnionOrcFiles

### spark-apps

Spark-submit app `com.github.vasnake.spark.app.ml-models.ApplyerApp`
and main workhorse for that app `com.github.vasnake.spark.ml.transformer.ApplyModelsTransformer`.
This app takes a batch of ML models, trained earlier in some 'learn' app, and apply them to each row of an input dataset (DataFrame).
Each ML model transform an input features vector to a score value, so that each input row transformed (exploded) to a batch of output rows.

Spark-submit app `com.github.vasnake.spark.app.datasets.JoinerApp` TBD.

### spark-ml

Three spark.ml packages (estimator + model). All three support stratification and sampling inside stratas.

`com.github.vasnake.spark.ml.estimator.ScoreEqualizerEstimator` + `com.github.vasnake.spark.ml.model.ScoreEqualizerModel`
Used for fixing values distribution.

`com.github.vasnake.spark.ml.estimator.NEPriorClassProbaEstimator` + `com.github.vasnake.spark.ml.model.NEPriorClassProbaModel`
Used for transforming scores to meet a given prior class label distribution, after ArgMax is applied.

`com.github.vasnake.spark.ml.estimator.ScoreQuantileThresholdEstimator` + `com.github.vasnake.spark.ml.model.ScoreQuantileThresholdModel`
Used for transforming regression scores to class labels, keeping class distribution close to a given prior distribution.

### Other

- core
    * com.github.vasnake.core.text.StringToolbox
    * com.github.vasnake.core.num.VectorToolbox
    * num-sci-py
        - com.github.vasnake.core.num.NumPy
        - com.github.vasnake.core.num.SciPy.PCHIP
    * com.github.vasnake.core.aggregation.TransformersPipeline

- common
    * com.github.vasnake.common.file.FileToolbox
    * com.github.vasnake.common.num.NumPy

- text // TODO: integrate with https://github.com/vasnake/join-expression-parser; add stack-based parser (based on python module); add tests
    * com.github.vasnake.text.parser.JoinExpressionParser
    * com.github.vasnake.text.evaluator.JoinExpressionEvaluator

- etl-core
    * com.github.vasnake.etl-core.GroupedFeatures

- ml-core
    * com.github.vasnake.ml-core.models.ScoreQuantileThreshold
    * com.github.vasnake.ml-core.models.Binarizer
    * com.github.vasnake.ml-core.models.GroupedFeaturesTfidf
    * com.github.vasnake.ml-core.models.Imputer
    * com.github.vasnake.ml-core.models.Scaler

- ml-models
    * com.github.vasnake.`ml-models`.complex.ComplexMLModel
    * com.github.vasnake.`ml-models`.complex.LalBinarizedMultinomialNb
    * com.github.vasnake.`ml-models`.complex.LalTfidfScaledSgdc
    * com.github.vasnake.`ml-models`.complex.ScoreAudience

- json
    * com.github.vasnake.json.JsonToolbox

- ml-models-json
    * com.github.vasnake.json.read.ModelConfig

- spark-transformers
    * com.github.vasnake.`etl-core`.aggregate.AggregationPipeline
    * com.github.vasnake.spark.features.vector.FeaturesRowDecoder
    * com.github.vasnake.spark.features.aggregate.ColumnAggregator
    * com.github.vasnake.spark.features.aggregate.DatasetAggregator#aggregateColumns
    * com.github.vasnake.spark.dataset.transform.StratifiedSamplerg#getGroupScoreSample
    * com.github.vasnake.spark.dataset.transform.Joiner#joinDatasets
    * com.github.vasnake.spark.dataset.transform.TopNRowsExact
    * com.github.vasnake.spark.dataset.transform.TopNRowsApprox

- spark-apps test (experiments)
    * com.github.vasnake.spark.app.interview.transform_array.InvalidValuesToNullApp
    * com.github.vasnake.spark.app.external_catalog.Alter_HMS_PartitionsApp
    * com.github.vasnake.spark.app.datasets.CompareDatasetsApp

## Unit tests

unit tests for each module
    * com.github.vasnake.core.num.*
    * com.github.vasnake.core.text.StringToolboxTest
    * com.github.vasnake.common.num.NumPyTest
    * com.github.vasnake.`etl-core`.GroupedFeaturesTest
    * com.github.vasnake.`ml-core`.models.*
    * com.github.vasnake.`ml-models`.complex.*
    * com.github.vasnake.spark.features.vector.FeaturesRowDecoderTest
    * com.github.vasnake.spark.io.hive.*
    * com.github.vasnake.spark.ml.transformer.*
    * com.github.vasnake.spark.ml.estimator.*
    * com.github.vasnake.spark.udf.`java-api`.*
    * org.apache.spark.sql.catalyst.vasnake.udf.*
    * org.apache.spark.sql.hive.vasnake.MetastoreQueryProcessorWithConnPoolTest
    * com.github.vasnake.spark.app.datasets.CompareDatasetsAppTest
    * com.github.vasnake.text.parser.JoinExpressionParserTest

## Integration tests, scripts

[See scripts directory](../scripts.python/readme.md)

## Spark notes

- https://spark.apache.org/news/index.html
- https://spark.apache.org/releases/
- [Project Matrix: Linear Models revisit and refactor / Blockification (vectorization of vectors)](https://issues.apache.org/jira/browse/SPARK-30641)
- [BLAS, LAPACK, Breeze, netlib-java acceleration](https://spark.apache.org/docs/latest/ml-linalg-guide.html#mllib-linear-algebra-acceleration-guide)

Some materials about Spark generic UDF/UDAF (and Catalyst)
- https://blog.cloudera.com/working-with-udfs-in-apache-spark/
- https://stackoverflow.com/questions/46474610/spark-udaf-generics-type-mismatch
- https://stackoverflow.com/questions/43248719/spark-udaf-using-generics-as-input-type
- https://stackoverflow.com/questions/32100973/how-to-define-and-use-a-user-defined-aggregate-function-in-spark-sql

Advanced Spark, Catalyst, Tangsten, etc
- Apache Spark Meetup 10-07-2015 https://www.youtube.com/watch?v=XxPXrMUXExM
- https://www.slideshare.net/slideshow/advanced-apache-spark-meetup-project-tungsten-nov-12-2015/55064824
- Care and Feeding of Catalyst Optimizer https://youtu.be/IjqC2Y2Hd5k?feature=shared
- Chris Fregly https://www.youtube.com/results?search_query=Chris+Fregly

## Build

My local station env: win11 + wsl2
```sh
# coursier update
cs setup

# goto project dir etl-ml-pieces-1923
pushd /mnt/c/Users/vlk/data/github/artefacts-2019_2023/etl-ml-pieces.scala/
# create new sbt project
sbt new devinsideyou/scala-seed.g8

# produced project with this parameters:
    name [etl-ml-pieces-1923]:
    organization [com.github.vasnake]:
    package [interesting.pieces.1923]:

# some tuning required ... edit sbt files

# start sbt console
alias sa='export JAVA_OPTS="-XX:MaxMetaspaceSize=1G -Xmx4G -XX:+UseParallelGC" && pushd /mnt/c/Users/vlk/data/github/artefacts-2019_2023/etl-ml-pieces.scala/ && sbt -v && popd'

sa
```

Sometimes I want to run sbt in PowerShell
```s
pushd ($env:HOMEDRIVE + $env:HOMEPATH + "\.")
pushd .\data\github\artefacts-2019_2023\etl-ml-pieces.scala\
$OutputEncoding = [console]::InputEncoding = [console]::OutputEncoding = New-Object System.Text.UTF8Encoding
sbt -v "-Dfile.encoding=UTF-8"
```

`build.sbt` tricks, see:
- https://github.com/DevInsideYou/tagless-final/blob/master/expression-problem/build.sbt
- https://github.com/tofu-tf/tofu/blob/master/build.sbt#L555

Other sbt-related resources:
- https://www.scala-sbt.org/1.x/docs/Multi-Project.html
- https://www.scala-lang.org/download/all.html
- https://docs.scala-lang.org/overviews/compiler-options/index.html#targeting-a-version-of-the-jvm
- https://scalacenter.github.io/scalafix/docs/users/installation.html#settings-and-tasks
- https://www.scalatest.org/user_guide/using_scalatest_with_sbt
- https://scastie.scala-lang.org/
- https://mvnrepository.com/artifact/org.unbescape/unbescape/1.1.6.RELEASE
- https://stackoverflow.com/questions/57521738/how-to-solve-sbt-dependency-problem-with-spark-and-whisklabs-docker-it-scala
- https://github.com/sbt/sbt-assembly

And a few more:
- set envvars for sbt: `export JAVA_OPTS="-XX:MaxMetaspaceSize=1G -Xmx4G -XX:+UseParallelGC" JAVA_HOME=$(/usr/libexec/java_home -v 1.8) && sbt -v`
- set envvars for tests: `sbt> set ThisBuild / Test / envVars := Map("DEBUG_MODE" -> "true", "SPARK_LOCAL_IP" -> "127.0.0.1")`; `sbt> set Test/logBuffered := false`
- select individual test: `sbt> testQuick *InverseVariabilityTransformer* -- -z "reference"`
- logs selectors/tuning: `edit test/resources/log4j*.properties`
