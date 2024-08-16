#!/bin/bash
# macos version

# run test spark job, local, for logging tuning;
# should output driver logs to stdout using logback or log4j

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

# cluster (need to run `bash -x run-spark-standalone.sh` first)
#export clusterMode='--master spark://localhost:7077 --deploy-mode cluster'
# OOPS, currently the standalone mode does not support cluster mode for Python applications
export clusterMode='--master spark://localhost:7077 --deploy-mode client'

# local
#export clusterMode='--master local[4] --deploy-mode client'

mkdir -p /tmp/spark-events

time "${SPARK_HOME}/bin/spark-submit" --verbose \
    --name "test-logging" \
    --class None \
    --jars "${UBER_JAR}" \
    ${clusterMode} \
    --total-executor-cores 4 --num-executors 4 --executor-cores 1 --executor-memory 1G --driver-memory 2G \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.speculation=false \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.dynamicAllocation.maxExecutors=4 \
    --conf spark.dynamicAllocation.minExecutors=4 \
    --conf spark.eventLog.enabled=true \
    --conf "spark.executor.extraJavaOptions=${extraJavaOptions}" \
    --conf "spark.driver.extraJavaOptions=${extraJavaOptions}" \
    --conf "spark.driver.userClassPathFirst=true" \
    --conf "spark.driver.extraClassPath=${UBER_JAR}" \
    --driver-class-path "${UBER_JAR}" \
    --files ~/${transformers_dir_h}/src/main/resources/log4j.properties \
    --driver-java-options "-Dlog4j.configuration=file:/${transformers_dir_f}/src/main/resources/log4j.properties" \
    pyspark_runner.py test_job

# test on yarn:
# bash -x spark-submit-cluster.sh 2> /tmp/stderr.log # you'll see only stdout on the screen

# spark params info

# spark.driver.userClassPathFirst	false, local: don't exists, for yarn only
# (Experimental) Whether to give user-added jars precedence over Spark's own jars when loading classes in the driver.

# spark.driver.extraClassPath	(none), local: --driver-class-path
# Extra classpath entries to prepend to the classpath of the driver.

#"${SPARK_HOME}/bin/spark-submit" --help
#  --conf PROP=VALUE           Arbitrary Spark configuration property.
#  --properties-file FILE      Path to a file from which to load extra properties. If not
#                              specified, this will look for conf/spark-defaults.conf.
#  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: 1024M).
#  --driver-java-options       Extra Java options to pass to the driver.
#  --driver-library-path       Extra library path entries to pass to the driver.
#  --driver-class-path         Extra class path entries to pass to the driver. Note that
#                              jars added with --jars are automatically included in the
#                              classpath.
