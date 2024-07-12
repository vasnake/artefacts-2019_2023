# Spark/Scala modules (mainly)

Collection of some interesting pieces from my projects.

WIP

Spark 2.4.8; Scala 2.12.19; sbt 1.10.0 (migration to Spark 3 is WIP)

My local station env (win11 + wsl2)
```sh
# coursier update
cs setup

# etl-ml-pieces-1923
pushd /mnt/c/Users/valik/data/github/artefacts-2019_2023/etl-ml-pieces.scala/
sbt new devinsideyou/scala-seed.g8

# produced project with this parameters:
    name [etl-ml-pieces-1923]:
    organization [com.github.vasnake]:
    package [interesting.pieces.1923]:
# some tuning required ...

# start project sbt
alias psbt='export JAVA_OPTS="-XX:MaxMetaspaceSize=1G -Xmx4G -XX:+UseParallelGC" && pushd /mnt/c/Users/valik/data/github/artefacts-2019_2023/etl-ml-pieces.scala/ && sbt -v && popd'

psbt
```
WSL shit

Sometimes I want to run sbt in PS
```s
pushd ($env:HOMEDRIVE + $env:HOMEPATH + "\.")
pushd .\data\github\artefacts-2019_2023\etl-ml-pieces.scala\
$OutputEncoding = [console]::InputEncoding = [console]::OutputEncoding = New-Object System.Text.UTF8Encoding
sbt -v "-Dfile.encoding=UTF-8"
```
PowerShell shit

For `build.sbt` tricks see
- https://github.com/DevInsideYou/tagless-final/blob/master/expression-problem/build.sbt
- https://github.com/tofu-tf/tofu/blob/master/build.sbt#L555

## What do we have here

Надо это как-то распихать по отдельным проектам билда.
Принцип деления: набор зависимостей, от ядра "нет зависимостей", до доменной логики "зависит от всего".

А еще куда-то как-то надо тесты интеграционные запихать. В виде скриптов и доки по использованию.

### dmscala-grinder_jobs
~\data\gitlab\dm.dmscala\dmscala-grinder_jobs\

- EtlFeaturesApp
- JoinExpressionParser
- DatasetAggregators
- DefaultHiveWriter
- CheckpointService
- toolboxes
    * file
    * string

### grinder_transformers
~\data\gitlab\dm.dmscala\dmscala\grinder_transformers\

- toolboxes
    * file
    * json
    * string
    * vector

- numpy-scipy
    * percentile
    * digitize
    * histogram
    * array_transpose
    * array_cumulativeSum
    * array_slice
    * pchip_coefficients

- catalog for all udf, udaf
- hive udf, udaf
- java udf aka spark.sql.api.java.UDF
    * MapValuesOrderedUDF
    * AggregateSumUDF
    * CosineSimilarityUDF
    * DotProductUDF
    * HtmlUnescapeUDF
    * DailyStatMapToArrayUDF
    * DailyStatActivityTypesUDF
- catalyst udf, udaf (spark.sql.catalyst.expressions)
    * GenericAvg
    * GenericIsFinite
    * GenericIsInf
    * GenericMax
    * GenericMin
    * GenericMostFreq
    * GenericSum
    * GenericVectorCooMul
    * GenericVectorMatMul
    * GenericVectorSemiDiff
    * GenericVectorSemiSum

- IO
    * hive.Writer
    * HiveExternalCatalog
    * MetastoreQueryProcessorWithConnPool

- ML models (model in this case is a pipeline of transformations and predictions):
    * LalBinarizedMultinomialNbModel
    * LalTfidfScaledSgdcModel
    * ScoreAudienceModel
- ML estimators and transformers
    * ScoreQuantileThreshold
    * Binarizer
    * TfIdf
    * Imputer
    * StandardScaler
    * SBGroupedTransformer
    * ScoreEqualizer
    * Slicer
    * MultinomialNB
    * PredictorWrapper
    * SGDClassifier

- spark.ml transformers and estimators (model, estimator); stratification, sampling
    * ArgMaxClassScore...
    * InverseVariability...
    * NEPriorClassProba...
    * ScoreEqualize...
    * ScoreQuantileThreshold...
    * ApplyModelsTransformer

- etl helpers (data adapters):
    * GroupedFeatures
    * (Grouped)FeaturesRowDecoder (convert grouped features to vector)

### proposed projects schema

- core (com.github.vasnake.core)
    * StringToolbox com.github.vasnake.core.text.StringToolbox
    * VectorToolbox com.github.vasnake.core.num.VectorToolbox
    * num-sci-py lib
        - com.github.vasnake.core.num.NumPy
        - com.github.vasnake.core.num.SciPy.PCHIP
    * configured aggregators (pipeline + config) from joiner com.github.vasnake.core.aggregation.TransformersPipeline

- common com.github.vasnake.common (apache commons)
    * FileToolbox com.github.vasnake.common.file.FileToolbox
    * numpy com.github.vasnake.common.num.NumPy

- text (parser combinators)
    * com.github.vasnake.text.parser.JoinExpressionParser
    * com.github.vasnake.text.evaluator.JoinExpressionEvaluator
    * TODO: integrate with https://github.com/vasnake/join-expression-parser; add stack-based parser (based on python module); add tests

- etl-core
    * grouped features com.github.vasnake.etl-core.GroupedFeatures

- ml-core (double, array[double], fit, transform: estimators, transformers)
    simple (one-stage) models
    * com.github.vasnake.ml-core.models.ScoreQuantileThreshold
    * com.github.vasnake.ml-core.models.Binarizer
    * com.github.vasnake.ml-core.models.GroupedFeaturesTfidf
    * com.github.vasnake.ml-core.models.Imputer
    * com.github.vasnake.`ml-core`.models.Scaler

- ml-models (Grinder complex models i.e. pipelines)
    * com.github.vasnake.`ml-models`.complex.GrinderMLModel
    * com.github.vasnake.`ml-models`.complex.LalBinarizedMultinomialNb
    * com.github.vasnake.`ml-models`.complex.LalTfidfScaledSgdcModel
    * com.github.vasnake.`ml-models`.complex.ScoreAudienceModel

- json
    * json toolbox

???
com.mrg.dm.toolbox.JsonToolbox
com.mrg.dm.grinder.loaders.ModelsConfigJson

- hive udf (java)

- spark-logging
    * logging

- spark-java-api udf
- spark-catalyst-api udf
- spark-udf-catalog

- spark-io
    * checkpoint (CheckpointService)
    * HDFSFileToolbox
    * hive (partition) writer (два, из джойнилки и из трансформеров: DefaultHiveWriter)
    * HiveExternalCatalog
    * MetastoreQueryProcessorWithConnPool

- spark-transformers
    * com.mrg.dm.grinder.features.CollectionColumnDecoder
    * column aggregator (configureg)
    * dataset aggregator (DatasetAggregators)
    * Stratified Sampling
    * joiner: from com.mrg.dm.grinder.jobs.etl_features.config.join.JoinRule

- spark-ml
    * transformers
    * estimators
    * models
    * params

- spark-apps
    * etl features (EtlFeaturesApp)
    * apply models transformer
