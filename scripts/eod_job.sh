#!/usr/bin/env bash

#########################
#@author hongbin@youzan.com
#@date
#@desc TODO
#########################
export PATH=/usr/bin:$PATH
export SCRIPT_PATH=`dirname $(readlink -f $0)` # get the path of the script
pushd . > /dev/null
cd "$SCRIPT_PATH"

cd .. && rm -rf eod.zip && zip -r eod.zip eod/ && cd -

/opt/spark/bin/spark-submit  --py-files ../eod.zip ../jobs/eod_job.py

popd  > /dev/null # return the directory orignal
