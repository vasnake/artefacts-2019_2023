#!/usr/bin/env bash
# mock for yarn app -list

# chmod +x yarn-app-list.sh
# ./yarn-app-list.sh

sleep 2
echo "application_1641762080783_70654	ApplyTask__ctid__prod_2806767	               SPARK	   jenkins	root.prod.regular	           RUNNING	         UNDEFINED	            10%	 http://rbhp422.host:36659"

# real deal should be like:
# args=( "$@" )
# echo yarn application list arguments: "${args[@]}"
# yarn application -appStates RUNNING -list | egrep 'MobAppAudienceTask_2022_01_16'
# yarn app -status MobAppAudienceTask_2022_01_16 -appTypes SPARK
