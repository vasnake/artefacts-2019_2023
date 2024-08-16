#!/bin/bash
# macos version

# test insert_to_hive on localhost

pushd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

setup_env_spark24_hive12_hadoop27() {

  # java
  [ -z "${JAVA_HOME}" ] && export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
  java_version=$(${JAVA_HOME}/bin/java -version 2>&1 | awk -F '"' '/version/ {print substr($2,0,3)}')
  if [ "$java_version" != "1.8" ]; then @echo "java 8 needed"; fi
  export PATH=${JAVA_HOME}/bin:$PATH

  # spark
  [ -z "${SPARK_HOME}" ] && export SPARK_HOME=${HOME}/.sparkenv/spark-2.4.3-bin-hadoop2.7
  export SPARK_LOCAL_HOSTNAME=localhost
  export PATH=${SPARK_HOME}/bin:$PATH

  # spark full classpath
  export SPARK_CLASSPATH=${SPARK_HOME}/jars/*
  export SPARK_DIST_CLASSPATH=${SPARK_CLASSPATH}
  export JAVA_CLASSPATH=${SPARK_CLASSPATH}
  export CLASSPATH=${SPARK_CLASSPATH}

}

# no need for spark.sql(ADD JAR ...)
run_spark_submit_with_jars() {
  spark-submit --verbose --master local[3] \
      --conf spark.sql.shuffle.partitions=3 \
      --conf spark.sql.warehouse.dir=/tmp/warehouse \
      --conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp \
      --conf spark.executor.extraJavaOptions=-Dderby.system.home=/tmp \
      --jars file:${SPARK_JARS} \
      writer_test.py
}

# use spark.sql(ADD JAR ...) in your code
run_spark_submit_no_jars() {
  spark-submit --verbose --master local[3] \
      --conf spark.driver.memory=1g \
      --conf spark.sql.shuffle.partitions=3 \
      --conf spark.sql.warehouse.dir=/tmp/warehouse \
      --conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp \
      --conf spark.executor.extraJavaOptions=-Dderby.system.home=/tmp \
      --conf spark.hadoop.dmgrinder.metastore.pool.size=3 \
      writer_test.py 1>/tmp/log.log 2>&1

  grep -n 'hive.Writer' /tmp/log.log
}

# custom jars
unset TEST_JARS  # if new version of jar is available
export TEST_JARS=$(find transformers/target/scala-2.11 -name '*-transformers-assembly-*.jar')
export SPARK_JARS=${TEST_JARS}

# vim $SPARK_HOME/conf/log4j.properties
# log4j.logger.org.apache.spark.sql.hive.HiveUtils=ALL
# log4j.logger.com.github.vasnake=DEBUG
setup_env_spark24_hive12_hadoop27
run_spark_submit_no_jars
