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

cd .. && rm -rf bintrade.zip && zip -r bintrade.zip see/ && cd -

/opt/spark/bin/spark-submit  --py-files ../bintrade.zip ./sma.py

popd  > /dev/null # return the directory orignal
