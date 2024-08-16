#!/bin/bash
# macos version

# run test spark job on localhost, scala-apply performance testing

[ -z "${JAVA_HOME}" ] && export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
java_version=$("${JAVA_HOME}/bin/java" -version 2>&1 | awk -F '"' '/version/ {print substr($2,0,3)}')
if [ "$java_version" != "1.8" ]; then @echo "java 8 needed"; fi
export PATH=${JAVA_HOME}/bin:$PATH
[ -z "${SPARK_HOME}" ] && export SPARK_HOME=${HOME}/.sparkenv/spark-2.4.3-bin-hadoop2.7
export SPARK_LOCAL_HOSTNAME=localhost
export SPARK_MASTER_HOST=localhost
export SPARK_WORKER_INSTANCES=2
export SPARK_MASTER_OPTS="-Dspark.deploy.defaultCores=4"

unset UBER_JAR
[ -z "${UBER_JAR}" ] && export UBER_JAR=$(find "../target/scala-2.11" -name '*-transformers-assembly-*.jar')
export SPARK_JARS=${UBER_JAR}

export JAVA_OPTS="-XX:MaxMetaspaceSize=1G -Xmx10G"
export extraJavaOptions="-XX:+UseCompressedOops -XX:hashCode=0"
# -XX:+PrintCompilation -XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining

# see src/test/scripts/run-spark-standalone.sh
#export clusterMode='--master spark://localhost:7077 --deploy-mode cluster'
export clusterMode='--master local[4] --deploy-mode client'

mkdir /tmp/spark-events

time "${SPARK_HOME}/bin/spark-submit" --verbose \
    --name "test-scala-apply" \
    --class "transformers.ApplyModelsJob" \
    --queue "default" \
    ${clusterMode} \
    --total-executor-cores 4 --num-executors 4 --executor-cores 1 --executor-memory 1G --driver-memory 2G \
    --conf spark.executor.memoryOverhead=2G \
    --conf spark.sql.shuffle.partitions=16 \
    --conf spark.hadoop.dfs.replication=1 \
    --conf spark.speculation=false \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.dynamicAllocation.maxExecutors=4 \
    --conf spark.dynamicAllocation.minExecutors=4 \
    --conf spark.sql.parquet.compression.codec=gzip \
    --conf spark.hadoop.zlib.compress.level=BEST_COMPRESSION \
    --conf spark.eventLog.enabled=true \
    --conf spark.driver.maxResultSize=2g \
    --conf spark.broadcast.blockSize=4m \
    --conf "spark.executor.extraJavaOptions=${extraJavaOptions}" \
    --conf "spark.driver.extraJavaOptions=${extraJavaOptions}" \
    --conf spark.checkpoint.dir=hdfs:///tmp \
    --conf spark.sql.sources.partitionColumnTypeInference.enabled=false \
    --conf spark.yarn.maxAppAttempts=1 \
    "${UBER_JAR}" \
        featuresIndex=/tmp/grouped_features.json \
        featuresDataset=/tmp/features.gz.parquet \
        numPartitions=4 \
        explodeFactor=5000 \
        repeat=2 \
        scalameter=off

#--conf spark.memory.offHeap.enabled=true \
#--conf spark.memory.offHeap.size=1000000000 \
#   --jars /etc/hive/conf/hive-site.xml \
