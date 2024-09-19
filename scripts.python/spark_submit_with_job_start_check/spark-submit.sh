#!/usr/bin/env bash
# mock for actual spark-submit

# chmod +x spark-submit.sh
# ./spark-submit.sh --master yarn --deploy-mode cluster --name ApplyTask__ctid__prod_2806767 --jars hdfs:/lib/prj-transformers-assembly-1.7.0.jar --files /tmp/tmpWrGplE/ApplyTask___py

#echo spark-submit arguments: "$@"

# save args
args=( "$@" )
echo spark-submit arguments: "${args[@]}"

sleep 2

# stdout line imitation
echo "23/03/29 17:17:54 INFO YarnClientImpl: Submitted application application_1680097008037_0263"

# stderr line imitation
>&2 echo "23/03/29 17:17:55 INFO Client: Application report for application_1680097008037_0263 (state: ACCEPTED)"

sleep 5

echo "spark-submit done"
echo -e '\007'
exit 0
