#!/bin/bash
# WSL Ubuntu version of the script

# test spark udf on localhost

pushd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" || exit

export PROJECT_DIR=/mnt/c/Users/vlk/my_project
export JAVA8_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK24_HOME=/mnt/c/bin/spark/spark-2.4.8-bin-hadoop2.7
export SPARK33_HOME=/mnt/c/bin/spark/spark-3.3.2-bin-hadoop3
export TMP_ROOT_DIR=/tmp

# sudo apt update && sudo apt install python2 && sudo update-alternatives --install /usr/bin/python python /usr/bin/python2.7 1
# sudo apt-get install python-pip && python2 -m pip install --upgrade pip six
PYTHON3=$(which python3) && export PYTHON3
PYTHON2=$(which python2) && export PYTHON2

# find ~ -name hive-*-2.1.1-cdh6.3.1*.jar 2>&1 | grep jar
# [ -z "${TEST_JARS}" ] && export TEST_JARS=$(find ${HOME}/Downloads/lib/DWH -name '*.jar')
unset TEST_JARS  # if new version of jar is available
TEST_JARS=$(find ${PROJECT_DIR}/transformers/target/scala-2.11 -name '*-transformers-assembly-*.jar')
export TEST_JARS
export SPARK_JARS=${TEST_JARS}

#rm -rf $TMP_ROOT_DIR/metastore_db
#rm -rf $TMP_ROOT_DIR/warehouse

setup_env_spark24_hive12_hadoop27() {

  # java
  [ -z "${JAVA_HOME}" ] && export JAVA_HOME=$JAVA8_HOME
  java_version=$(${JAVA_HOME}/bin/java -version 2>&1 | awk -F '"' '/version/ {print substr($2,0,3)}')
  if [ "$java_version" != "1.8" ]; then @echo "java 8 needed"; fi
  export PATH=${JAVA_HOME}/bin:$PATH
  export JRE_HOME=${JAVA_HOME}

  # spark
  [ -z "${SPARK_HOME}" ] && export SPARK_HOME=$SPARK24_HOME
  export SPARK_LOCAL_HOSTNAME=localhost
  export PATH=${SPARK_HOME}/bin:$PATH
  export PYSPARK_PYTHON=$PYTHON2

  # spark.sql.hive.metastore.jars
  export HIVE_METASTORE_JARS=maven

  # spark full classpath (deprecated)
  export SPARK_CLASSPATH=${SPARK_HOME}/jars/*
  export SPARK_DIST_CLASSPATH=${SPARK_CLASSPATH}
  export JAVA_CLASSPATH=${SPARK_CLASSPATH}
  export CLASSPATH=${SPARK_CLASSPATH}

}

setup_env_spark33_hive23_hadoop3() {
  # java
  [ -z "${JAVA_HOME}" ] && export JAVA_HOME=$JAVA8_HOME
  java_version=$(${JAVA_HOME}/bin/java -version 2>&1 | awk -F '"' '/version/ {print substr($2,0,3)}')
  if [ "$java_version" != "1.8" ]; then @echo "java 8 needed"; fi
  export PATH=${JAVA_HOME}/bin:$PATH
  export JRE_HOME=${JAVA_HOME}

  # spark
  [ -z "${SPARK_HOME}" ] && export SPARK_HOME=$SPARK33_HOME
  export SPARK_LOCAL_HOSTNAME=localhost
  export PATH=${SPARK_HOME}/bin:$PATH

  # spark.sql.hive.metastore.jars
  export HIVE_METASTORE_JARS=maven

  # spark full classpath
  export SPARK_CLASSPATH=${SPARK_HOME}/jars/*
  export SPARK_DIST_CLASSPATH=${SPARK_CLASSPATH}
  export JAVA_CLASSPATH=${SPARK_CLASSPATH}
  export CLASSPATH=${SPARK_CLASSPATH}

}

# use spark.sql(ADD JAR ...) in your code
run_spark_submit_no_jars() {

  time spark-submit --verbose --master local[3] \
      --conf spark.sql.shuffle.partitions=3 \
      --conf spark.sql.warehouse.dir=$TMP_ROOT_DIR/warehouse \
      --conf spark.driver.extraJavaOptions=-Dderby.system.home=$TMP_ROOT_DIR \
      --conf spark.executor.extraJavaOptions=-Dderby.system.home=$TMP_ROOT_DIR \
      java_udf_test.py

}

run_spark_submit_with_jars() {

  time spark-submit --verbose --master local[3] \
      --conf spark.sql.shuffle.partitions=3 \
      --conf spark.sql.warehouse.dir=$TMP_ROOT_DIR/warehouse \
      --conf spark.driver.extraJavaOptions=-Dderby.system.home=$TMP_ROOT_DIR \
      --conf spark.executor.extraJavaOptions=-Dderby.system.home=$TMP_ROOT_DIR \
      --jars file:${SPARK_JARS} \
      java_udf_test.py

}

# vim $SPARK_HOME/conf/log4j2.properties
# log4j.logger.org.apache.spark.sql.hive.HiveUtils=ALL
#logger.vasnake.name = com.github.vasnake
#logger.vasnake.level = debug

setup_env_spark24_hive12_hadoop27
run_spark_submit_no_jars

echo -e '\a' # beep
