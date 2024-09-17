#!/bin/bash
# alias at='bash -x ~/gitlab/prj/.local/run_tests.sh' #useful

save_it_tests() {
  src_base_dir=~/gitlab/prj
  trg_base_dir=~/github/vasnake/prj_aux

  rsync -arhv ${src_base_dir}/.local/*.sh ${trg_base_dir}/

  rsync -arhv ${src_base_dir}/prj/apps/apply/test/it ${trg_base_dir}/apps/apply/test/

  rsync -arhv ${src_base_dir}/prj/apps/export/test/it ${trg_base_dir}/apps/export/test/
  rsync -arhv ${src_base_dir}/prj/apps/export/audience/trg/test/it ${trg_base_dir}/apps/export/audience/trg/test/
  rsync -arhv ${src_base_dir}/prj/apps/export/audience/rb/test/it ${trg_base_dir}/apps/export/audience/rb/test/
  rsync -arhv ${src_base_dir}/prj/apps/export/universal_features/test/it ${trg_base_dir}/apps/export/universal_features/test/
  rsync -arhv ${src_base_dir}/prj/apps/export/ad_features/test/it ${trg_base_dir}/apps/export/ad_features/test/

  rsync -arhv ${src_base_dir}/prj/apps/combine/universal_features/test/it ${trg_base_dir}/apps/combine/universal_features/test/
  rsync -arhv ${src_base_dir}/prj/apps/combine/ad_features/test/it ${trg_base_dir}/apps/combine/ad_features/test/

  mkdir -p ${trg_base_dir}/apps/joiner/features/test
  rsync -arhv ${src_base_dir}/prj/apps/joiner/features/test/it ${trg_base_dir}/apps/joiner/features/test/

  mkdir -p ${trg_base_dir}/apps/utils/control
  rsync -arhv ${src_base_dir}/prj/apps/utils/control/test ${trg_base_dir}/apps/utils/control/

  mkdir -p ${trg_base_dir}/apps/finder/custom/test
  rsync -arhv ${src_base_dir}/prj/apps/finder/custom/test/it ${trg_base_dir}/apps/finder/custom/test/
}

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
  # vim ${HOME}/.sparkenv/spark-2.4.3-bin-hadoop2.7/conf/log4j.properties

  # spark full classpath
  export SPARK_CLASSPATH=${SPARK_HOME}/jars/*
  export SPARK_DIST_CLASSPATH=${SPARK_CLASSPATH}
  export JAVA_CLASSPATH=${SPARK_CLASSPATH}
  export CLASSPATH=${SPARK_CLASSPATH}

  # custom jar, local build
  unset TEST_JARS  # if new version of jar is available
  export TEST_JARS=$(find ${HOME}/gitlab/prj/custom_transformers/target/scala-2.11 -name 'custom-transformers-assembly-*.jar')

# custom jars, downloaded from hadoop
#  rm -v ./proto2-1.28.2.jar; hdfs dfs -copyToLocal /lib/dwh/proto2-1.28.2.jar
#  gdfs put ~/proto2-1.28.2.jar /user/$USER/
#  fshare /user/$USER/proto2-1.28.2.jar

  #  hdfs:/data/dm/lib/brickhouse-0.7.1-SNAPSHOT.jar
  #  hdfs:/data/dm/lib/DWH/common-1.0-SNAPSHOT.jar # /data/dm/lib/DWH/common-1.0-SNAPSHOT.jar
  #  hdfs:/lib/dwh/proto2-1.22.5.jar
  #  hdfs:/lib/dwh/common-1.16.11.jar

  export brickhouse_jar=${HOME}/Downloads/brickhouse-0.7.1-SNAPSHOT.jar
  export proto2_jar=${HOME}/Downloads/proto2-1.28.2.jar
  export common_jar=${HOME}/Downloads/common-1.21.7.jar
  export common_snapshot_jar=${HOME}/Downloads/common-1.0-SNAPSHOT.jar
  export hadoop_streaming_jar=${HOME}/.sparkenv/hadoop-2.9.2/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar

  export SPARK_JARS="${TEST_JARS},${brickhouse_jar},${common_jar},${hadoop_streaming_jar},${common_snapshot_jar}"
  export ADD_JAR="${proto2_jar}"

  # prj
  export CONTROL_DEBUG=true
  export CONTROL_DEBUG_FS=local
  export CLICKHOUSE_DEBUG=true

#  export LOKY_PICKLER=dill

  # spark
  export PYTEST_SPARK_PARALLELISM=3
#  vim ~/.sparkenv/spark-2.4.3-bin-hadoop2.7/conf/log4j.properties
}

e2e_debug() {

  export CONTROL_DEBUG=true
  export CONTROL_DEBUG_FS=local
  export CLICKHOUSE_DEBUG=true
  export LUIGI_CONFIG_PATH=prj/apps/joiner/features/config/config_test_e2e.cfg

  pipenv run python -m pytest -svv --tb=short --log-cli-level=WARNING --pyargs prj.apps.joiner.features.test.e2e || exit

}

_exit() {
  # https://madflojo.medium.com/understanding-exit-codes-in-bash-6942a8b96ce5
  # echo "Last Exit Code" $?

  if [ $? -eq 0 ]
  then
    say "Rise And Shine!"
  else
    say "You've Got An Error"
  fi

  exit
}

# copy files to aux repository
save_it_tests

# setup environment for prj IT suite
setup_env_spark24_hive12_hadoop27

# tests in docker
#bash -x ~/gitlab/dm.prj/prj/.local/run_tests_in_docker_.sh; _exit
# NB, after tests in docker with mounted project dir, you should cleanup a little:
# find . -type f -name *.pyc -delete 2>&1

# luigi and Control mechanics probe
#e2e_debug

export PYTEST_OPTIONS="-x -svv --tb=short --disable-warnings --log-cli-level=WARNING"

# finder/custom

pipenv run python -B -m pytest prj/apps/utils/common/test/local -k "TestJoinFilter" $PYTEST_OPTIONS || _exit

pipenv run python -B -m pytest prj/apps/utils/common/test/local -k "TestFindPartitionsEngine" $PYTEST_OPTIONS || _exit

LUIGI_CONFIG_PATH=prj/apps/finder/custom/config/config_test_e2e.cfg \
  pipenv run python -B -m pytest prj/apps/finder/custom/test/it -k "TestCustomFinderTask" $PYTEST_OPTIONS || _exit

_exit

# export/combine apps

pipenv run python -m pytest prj/apps/combine/universal_features/test/it -k "TestCombineUniversalFeaturesTask" $PYTEST_OPTIONS || _exit
pipenv run python -m pytest prj/apps/combine/ad_features/test/it -k "TestCombineAdFeaturesTask" $PYTEST_OPTIONS || _exit

pipenv run python -B -m pytest prj/apps/export/ad_features/test/it -k "TestExportAdFeaturesTask" $PYTEST_OPTIONS || _exit
pipenv run python -B -m pytest prj/apps/export/universal_features/test/it -k "TestExportUniversalFeatures" $PYTEST_OPTIONS || _exit

pipenv run python -B -m pytest prj/apps/export/audience/rb/test/it $PYTEST_OPTIONS || _exit
LUIGI_CONFIG_PATH=prj/apps/export/audience/trg/test/it/luigi_config.cfg \
  pipenv run python -B -m pytest prj/apps/export/audience/trg/test/it $PYTEST_OPTIONS || _exit
#_exit

# joiner_features

LUIGI_CONFIG_PATH=prj/apps/joiner/features/config/config_test_e2e.cfg \
  pipenv run python -B -m pytest $PYTEST_OPTIONS \
  -k "TestJoinerFeaturesTask" prj/apps/joiner/features/test/it  || _exit

#pipenv run python -B -m pytest prj/apps/joiner/features/test/it/test_missing_partitions.py -k "test_missing_partitions" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -B -m pytest prj/apps/joiner/features/test/it/test_meta.py -k "TestLuigiPipeline and test_meta_inject" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -B -m pytest prj/apps/joiner/features/test/it/test_meta.py -k "TestLuigiPipeline and test_parallel_finish" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/joiner/features/test/it -k "test_report_runs_exception" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/joiner/features/test/it -k "TestHiveIO" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/joiner/features/test/it -k "TestSignals" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/joiner/features/test/it/test_luigi.py -k "TestLuigiWrapper" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/joiner/features/test/it -k "TestJoinRule" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/joiner/features/test/it -k "TestCreateHiveTableDDL" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit

#pipenv run python -B -m prj.apps.utils.control.it.test_workers
#pipenv run python -B -m prj.apps.utils.control.it.test_context

#pipenv run python -m pytest prj/apps/utils/control/luigix/test/local -k "TestControlTask and test_app_run" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/utils/control/luigix/test/local -k "TestControlTask and test_task_methods" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit

#pipenv run python -m pytest prj/apps/etl_features/test/local -k "TestPartitionsSearch" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/etl_features/test/local -k "TestETLFeaturesTask" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit

#pipenv run python -m pytest prj/apps/utils/common/test/local -k "TestSparkJarLib" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit

_exit

# main test set

pipenv run python -m pytest prj/apps/cleaner/test/local -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest prj/apps/etl_features/test/local -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest prj/apps/utils/common/test -k "TestSparkJarLib" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest prj/apps/utils/common/test/local -k "TestSkewedJoin" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest prj/apps/utils/common/test -k "TestFindPartitionsEngine and is_valid_dts" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest prj/apps/apply -k "local" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest prj/sparkml -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit

_exit

# extra set

#pipenv run python -m pytest prj/apps/apply/test/it -k "not apply_speed" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/apply/test/it -k "apply_speed" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest prj/adapters -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest prj/apps -k "local" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest dmcore/test -k "ArgMaxClassScoreTransformer" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest dmcore/test -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit

_exit

# sparkml transformers

pipenv run python -m pytest prj/sparkml -k "TestInverseVariabilityTransformer" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest prj/sparkml -k "TestScoreEqualizeTransformer" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest prj/sparkml -k "TestScoreQuantileThresholdTransformer" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest prj/sparkml -k "TestArgMaxClassScoreTransformer" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
pipenv run python -m pytest prj/sparkml -k "TestNEPriorClassProbaTransformer" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit

_exit

#pipenv run python -m pytest prj/apps/combine/ad_features/test/it -k "TestGenerateE2EFixture and export_source_spec" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/export/ad_features/test/it -k "test_build_features_list" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit

#pipenv run python -m pytest prj/apps/utils/common -k "FindPartitions" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest -sv --tb=short --log-cli-level=INFO --pyargs prj.apps.utils

#pipenv run python -m pytest prj/sparkml -k "TestModelsApplyTransformer" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/adapters -k "TestApplyAdapters" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/apply/test/it -x -k "TestApplyTask" -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit

#pipenv run python -m pytest prj/apps/apply/test/it -x -k "test_apply_e2e_imitation" -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/adapters -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/sparkml -k "not TestModelsApplyTransformer" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps -k "local" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit

#pipenv run python -m pytest prj/sparkml -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/adapters -k "TestGroupedFeaturesAdapter" -x -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit

#pipenv run python -m pytest prj/apps/apply/test/local -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/adapters -k "construct_deconstruct" -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/apply/test/it -k "apply_full_config" -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/apply/test/it -k "compare_models" -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
#pipenv run python -m pytest prj/apps/etl_features/test/local -svv --tb=short --disable-warnings --log-cli-level=WARNING || exit
