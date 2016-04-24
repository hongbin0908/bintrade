#!/usr/bin/env bash
#########################
#@author hongbin@youzan.com
#@date
#@desc TODO
#########################
export PATH=/usr/bin:$PATH
export SCRIPT_PATH=`dirname $(readlink -f $0)` # get the path of the script

cd "$SCRIPT_PATH"


cd .. && find . -name "*.pyc" | xargs -i rm -rf {} && rm -rf bintrade.zip && zip -r bintrade.zip *  && cd -

spark-submit --py-files ../bintrade.zip eod_run.py

exit $?
