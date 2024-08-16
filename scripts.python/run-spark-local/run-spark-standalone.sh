#!/bin/bash
# macos version

# start spark standalone cluster

[ -z "${JAVA_HOME}" ] && export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
java_version=$("${JAVA_HOME}/bin/java" -version 2>&1 | awk -F '"' '/version/ {print substr($2,0,3)}')
if [ "$java_version" != "1.8" ]; then @echo "java 8 needed"; fi

export PATH=${JAVA_HOME}/bin:$PATH

[ -z "${SPARK_HOME}" ] && export SPARK_HOME=${HOME}/.sparkenv/spark-2.4.3-bin-hadoop2.7

export SPARK_LOCAL_HOSTNAME=localhost
export SPARK_MASTER_HOST=localhost
export SPARK_WORKER_INSTANCES=2
export SPARK_MASTER_OPTS="-Dspark.deploy.defaultCores=4"

bash -x "${SPARK_HOME}/sbin/start-master.sh"
sleep 3
bash -x "${SPARK_HOME}/sbin/start-slave.sh" spark://localhost:7077

#  stop
#bash -x "${SPARK_HOME}/sbin/stop-all.sh"
#pkill -lf worker.Worker
