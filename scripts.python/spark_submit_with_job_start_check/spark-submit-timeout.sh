#!/usr/bin/env bash
# replacement for spark-submit, adds check if job is actually accepted by yarn, with timeout
# In case of drop-in replacement (script named `spark-submit`) you have to add a parameter: path to original spark-submit

# chmod +x spark-submit-timeout.sh
# ./spark-submit-timeout.sh --master yarn --deploy-mode cluster --name ApplyTask__ctid__prod_2806767 --jars hdfs:/lib/prj-transformers-assembly-1.7.0.jar

# save args
args=( "$@" )
echo spark-submit-timeout arguments: "${args[@]}"

# find app name
APP_NAME=
while [ $# -gt 0 ]; do
   if [[ $1 == "--name" ]]; then
        APP_NAME=$2
   fi
  shift
done
if [ -z ${APP_NAME} ];
then
  echo "NO app name is found"
  exit 1
else
  echo "app name: ${APP_NAME}"
  export APP_NAME
fi

# TODO: use `mktemp` or current pid `$$` for making unique file names
export SPARK_SUBMIT_TIMEOUT_STDOUT_LOG=/tmp/spark-submit-stdout-${APP_NAME}.log
export SPARK_SUBMIT_TIMEOUT_STDERR_LOG=/tmp/spark-submit-stderr-${APP_NAME}.log

# launch submit in background
# TODO: replace `./spark-submit.sh` mock with actual `spark-submit`
(./spark-submit.sh "${args[@]}" 1> >(tee ${SPARK_SUBMIT_TIMEOUT_STDOUT_LOG}) 2> >(tee ${SPARK_SUBMIT_TIMEOUT_STDERR_LOG} >&2)) &
pid=$!
echo "spark-submit PID: ${pid}"

check_pid_is_running () {
  # return 0 if spark-submit process is running (ec: exit code)
  ps -p $pid
  ec=$?
  echo "ps -p $pid => $ec"
  return $ec
}

check_job_not_running_yarn () {
  # chack yarn status
  # TODO: replace `./yarn-app-list.sh` mock with actual `yarn` command
  (timeout 60s ./yarn-app-list.sh application -appStates RUNNING -list) | grep -q -F "${APP_NAME}"
  ec=$?
  echo "yarn, grep exit code ${ec}"
  return $ec
}

check_job_not_running_log () {
  # check spark-submit logs
  grep -q -F "state: ACCEPTED" "${SPARK_SUBMIT_TIMEOUT_STDERR_LOG}" "${SPARK_SUBMIT_TIMEOUT_STDOUT_LOG}"
  ec=$?
  echo "log, grep exit code ${ec}"
  return $ec
}

check_job_not_running () {
  # return 0 if job is not running
  res=0
  echo "checking spark job ${APP_NAME} ..."
  # two options
  # check_job_not_running_yarn
  check_job_not_running_log
  ec=$?

  case $ec in
    0) echo "running"
      res=1;;
    1) echo "not running";;
    2) echo "file not found";;
    *) echo "WTF? grep return code ${ec}";;
  esac

  return $res
}

# periodically check if job started, until timeout

# max number of checks, 15 by default
number_of_checks=${SPARK_SUBMIT_TIMEOUT_PROBES:-15}

# sleep between checks, seconds, 60 by default
check_interval=${SPARK_SUBMIT_TIMEOUT_SLEEP:-60}

echo "spark-submit timeout = ${number_of_checks} probes * ${check_interval} sleep_seconds"

# zero considered as true
job_not_running=0
pid_is_running=0

# iteration, probe number
i=0

#while pid_is_running ; [[ $? -eq 0 && $number_of_checks -gt $i && $job_not_running -eq 1 ]] ; do
while (( pid_is_running == 0 && i < number_of_checks && job_not_running == 0 )); do
  ((i++))
  echo "probe $i"
  sleep $check_interval

  check_job_not_running
  job_not_running=$?

  check_pid_is_running
  pid_is_running=$?
done

# This is the point where: process terminated or job started or time is out

# if process still running but job not started: abort
if (( pid_is_running == 0 && job_not_running == 0 ));
then
  echo "job $APP_NAME can't start, terminating ..."
  kill -SIGTERM $pid; sleep 3; kill -SIGKILL $pid
  exit 1
fi

echo "waiting for PID $pid ..."
wait ${pid}
exit_code=$?
echo "PID $pid is finished, exit code $exit_code"; echo -e '\007'

echo "spark stderr:"
cat ${SPARK_SUBMIT_TIMEOUT_STDERR_LOG}
echo "spark stdout:"
cat ${SPARK_SUBMIT_TIMEOUT_STDOUT_LOG}

rm -vf "${SPARK_SUBMIT_TIMEOUT_STDERR_LOG}" "${SPARK_SUBMIT_TIMEOUT_STDOUT_LOG}"

exit $exit_code
