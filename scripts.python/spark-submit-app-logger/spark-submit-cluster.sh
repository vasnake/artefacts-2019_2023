#!/bin/bash
# centos version

# run test spark job, on cluster, for logging tuning, output driver logs to stdout

UBER_JAR=hdfs:/${PWD}/transformers-assembly-SNAPSHOT.jar
CLUSTERMODE='--master yarn --deploy-mode cluster'
EXTRAJAVAOPTIONS="-XX:+UseCompressedOops -XX:hashCode=0 -Dlog4j.debug=true"

spark-submit --verbose ${CLUSTERMODE} --name "test-logging" \
    --class None \
    --jars "${UBER_JAR}" \
    --total-executor-cores 2 --num-executors 2 --executor-cores 1 --executor-memory 1G --driver-memory 1G \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf spark.sql.shuffle.partitions=2 \
    --conf spark.speculation=false \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.dynamicAllocation.maxExecutors=2 \
    --conf spark.dynamicAllocation.minExecutors=2 \
    --conf spark.eventLog.enabled=true \
    --conf "spark.executor.extraJavaOptions=${EXTRAJAVAOPTIONS}" \
    --conf "spark.driver.extraJavaOptions=${EXTRAJAVAOPTIONS}" \
    --files ./log4j.properties \
    pyspark_runner.py test_job

# both ok:
#    --files ./log4j.properties \
#    --files hdfs:/user/$USER/log4j.properties \

# usless options:
#    --conf "spark.driver.userClassPathFirst=true" \
#    --conf "spark.driver.extraClassPath=${UBER_JAR}" \
#    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=./log4j.properties \
#    --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=./log4j.properties \
#    --driver-class-path "${UBER_JAR}" \
#    --driver-java-options "-Dlog4j.configuration=file:/${PWD}/transformers/src/main/resources/log4j.properties" \

# May be useful:
# > To specify a different configuration directory other than the default “SPARK_HOME/conf”, you can set SPARK_CONF_DIR.
# > Spark will use the configuration files (spark-defaults.conf, spark-env.sh, log4j.properties, etc) from this directory.
