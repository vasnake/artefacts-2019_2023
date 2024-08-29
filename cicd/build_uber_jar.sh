#!/bin/bash
set -ex

SBT_PROJECT_DIR=$1 # ../etl-ml-pieces.scala
PACKAGES_DIR=$2 # /tmp/workdir/PACKAGES

# debug
echo SBT_PROJECT_DIR ${SBT_PROJECT_DIR}
echo PACKAGES_DIR ${PACKAGES_DIR}
pwd
ls -la
whoami
env
java -version
javac -version

if [ ! -f "${SBT_PROJECT_DIR}/build.sbt" ]; then
    echo "Unknown sbt project, check the first argument"
    exit 1
fi

if [ -z "${PACKAGES_DIR}" ]; then
    echo "Target dir must be given, check the second argument"
fi

export JAVA_OPTS="${JAVA_OPTS} -Dsbt.log.noformat=true"

pushd "${SBT_PROJECT_DIR}"
timeout -v -k 5s 30m sbt -v --mem 4096 clean compile test assembly
#    -k duration
#   --kill-after=duration
popd

mkdir -p ${PACKAGES_DIR}
cp "${SBT_PROJECT_DIR}/target/scala-2.11/*.jar" ${PACKAGES_DIR}
# target/scala-2.11/etl-ml-pieces-1923-assembly-0.1.0.jar

# debug
ls -lh ${PACKAGES_DIR}
