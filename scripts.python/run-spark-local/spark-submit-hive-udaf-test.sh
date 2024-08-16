#!/bin/bash
# macos version

# test hive udaf on localhost

pushd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

#export JAVA_HOME=$(dirname $(dirname $(readlink $(readlink $(which java)))))
#export SPARK_JARS=$(find /usr/local/*/ -maxdepth 1 -type f -name *.jar | paste -s -d",")
#export PYSPARK_PYTHON=$(pwd)/${CONDA_PYTHON_RELPATH}

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

setup_env_spark24_hive21_hadoop29() {
  # java
  [ -z "${JAVA_HOME}" ] && export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
  java_version=$(${JAVA_HOME}/bin/java -version 2>&1 | awk -F '"' '/version/ {print substr($2,0,3)}')
  if [ "$java_version" != "1.8" ]; then @echo "java 8 needed"; fi
  export PATH=${JAVA_HOME}/bin:$PATH

  # spark
  [ -z "${SPARK_HOME}" ] && export SPARK_HOME=${HOME}/.sparkenv/spark-2.4.3-bin-without-hadoop
  export SPARK_HADOOP=${HOME}/.sparkenv/spark-2.4.3-bin-hadoop2.7
  export SPARK_LOCAL_HOSTNAME=localhost
  export PATH=${SPARK_HOME}/bin:$PATH

  # hive
  export HADOOP_HOME=${HOME}/.sparkenv/hadoop-2.9.2
  #export HADOOP_COMMON_HOME=${HADOOP_HOME}/share/hadoop
  export HADOOP_CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath)
  export HIVE_HOME=${HOME}/.sparkenv/apache-hive-2.1.1-bin
  export HIVE_JARS=${HIVE_HOME}/lib
  export HIVE_METASTORE_JARS=${HIVE_JARS}/*:${HOME}/.sparkenv/hive-2.1.1/*
  export PATH=${HADOOP_HOME}/bin:${HIVE_HOME}/bin:$PATH

  # recreate hive metastore
  rm -rfv /tmp/DM-8709; mkdir -p /tmp/DM-8709; pushd /tmp/DM-8709 && schematool --dbType derby --initSchema && popd || exit

  # spark full classpath
  # TODO: get rid of SPARK_HADOOP
  export SPARK_CLASSPATH=${SPARK_HADOOP}/jars/*:${SPARK_HOME}/jars/*:${HIVE_JARS}/*:${HADOOP_CLASSPATH}:${HOME}/.sparkenv/hive-2.1.1/*
  export SPARK_DIST_CLASSPATH=${SPARK_CLASSPATH}
  export JAVA_CLASSPATH=${SPARK_CLASSPATH}
  export CLASSPATH=${SPARK_CLASSPATH}
}

run_spark_submit_test_hive21() {

  spark-submit --verbose --master local[1] \
      --conf spark.sql.shuffle.partitions=1 \
      --conf spark.sql.warehouse.dir=/tmp/DM-8709/warehouse \
      --conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/DM-8709 \
      --conf spark.executor.extraJavaOptions=-Dderby.system.home=/tmp/DM-8709 \
      --conf spark.sql.hive.metastore.version=2.1.1 \
      --conf spark.sql.hive.metastore.jars=${HIVE_METASTORE_JARS} \
      --conf spark.sql.catalogImplementation=hive \
      --jars file:${SPARK_JARS} \
      ${HOME}/hive_udaf_test.py

}

run_spark_submit_test_hive12() {

  spark-submit --verbose --master local[3] \
      --conf spark.sql.shuffle.partitions=3 \
      --conf spark.sql.warehouse.dir=/tmp/warehouse \
      --conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp \
      --conf spark.executor.extraJavaOptions=-Dderby.system.home=/tmp \
      --jars file:${SPARK_JARS} \
      ${HOME}/hive_udaf_test.py

}

experiments() {

    # java
    [ -z "${JAVA_HOME}" ] && export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
    java_version=$(${JAVA_HOME}/bin/java -version 2>&1 | awk -F '"' '/version/ {print substr($2,0,3)}')
    if [ "$java_version" != "1.8" ]; then @echo "java 8 needed"; fi
    export PATH=${JAVA_HOME}/bin:$PATH

    # spark
    # spark-2.4.3-bin-without-hadoop
    # spark-2.4.3-bin-hadoop2.7
    [ -z "${SPARK_HOME}" ] && export SPARK_HOME=${HOME}/.sparkenv/spark-2.4.3-bin-without-hadoop
    export SPARK_LOCAL_HOSTNAME=localhost
    export PATH=${SPARK_HOME}/bin:$PATH
    export SPARK_HADOOP=${HOME}/.sparkenv/spark-2.4.3-bin-hadoop2.7

    # hive
    export HADOOP_HOME=${HOME}/.sparkenv/hadoop-2.9.2
    #export HADOOP_COMMON_HOME=${HADOOP_HOME}/share/hadoop
    export HIVE_HOME=${HOME}/.sparkenv/apache-hive-2.1.1-bin
    export HADOOP_CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath)
    export HIVE_JARS=${HIVE_HOME}/lib
    #export HIVE_METASTORE_JARS=${HOME}/.sparkenv/hive-2.1.1/*
    export HIVE_METASTORE_JARS=${HIVE_JARS}/*:${HOME}/.sparkenv/hive-2.1.1/*
    export PATH=${HADOOP_HOME}/bin:${HIVE_HOME}/bin:$PATH
    # recreate hive metastore
    rm -rfv /tmp/DM-8709; mkdir -p /tmp/DM-8709; pushd /tmp/DM-8709 && schematool --dbType derby --initSchema && popd || exit

    # spark full classpath
    # TODO: надо избавиться от SPARK_HADOOP, должно работать без него!
    export SPARK_CLASSPATH=${SPARK_HADOOP}/jars/*:${SPARK_HOME}/jars/*:${HIVE_JARS}/*:${HADOOP_CLASSPATH}:${HOME}/.sparkenv/hive-2.1.1/*
    #export SPARK_CLASSPATH=${SPARK_HOME}/jars/*:${HIVE_JARS}/*:${HADOOP_CLASSPATH}:${HOME}/.sparkenv/hive-2.1.1/*:${SPARK_HADOOP}/jars/*
    export SPARK_DIST_CLASSPATH=${SPARK_CLASSPATH}
    export JAVA_CLASSPATH=${SPARK_CLASSPATH}
    export CLASSPATH=${SPARK_CLASSPATH}

}

# custom jars
# find ~ -name hive-*-2.1.1-cdh6.3.1*.jar 2>&1 | grep jar
unset TEST_JARS  # if new version of jar is available
export TEST_JARS=$(find ${HOME}/transformers/target/scala-2.11 -name '*-transformers-assembly-*.jar')
#[ -z "${TEST_JARS}" ] && export TEST_JARS=$(find ${HOME}/Downloads/lib/DWH -name '*.jar')
export SPARK_JARS=${TEST_JARS}

# vim $SPARK_HOME/conf/log4j.properties
# log4j.logger.org.apache.spark.sql.hive.HiveUtils=ALL
# log4j.logger.org.apache.spark.sql.catalyst.vasnake=DEBUG
# log4j.logger.com.github.vasnake=DEBUG
setup_env_spark24_hive12_hadoop27
run_spark_submit_test_hive12
