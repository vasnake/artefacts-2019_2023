# Spark/Scala modules (mainly)

Collection of some interesting pieces from my projects.

Spark 2.4.8; Scala 2.12.19; sbt 1.10.0

My local station env (win11 + wsl2)
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

## What do we have here

Надо это как-то распихать по отдельным проектам билда.
Принцип деления: набор зависимостей, от ядра "нет зависимостей", до доменной логики "зависит от всего".

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
