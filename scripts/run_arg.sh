#!/usr/bin/env bash

#########################
#@author hongbin@youzan.com
#@date
#@desc TODO
#########################
export PATH=/usr/bin:$PATH
export SCRIPT_PATH=`dirname $(readlink -f $0)` # get the path of the script
CURR=`pwd`
cd "$SCRIPT_PATH"


cd .. && find . -name "*.pyc" | xargs -i rm -rf {} && rm -rf bintrade.zip && zip -r bintrade.zip * &> /dev/null && cd -

cd $CURR
spark-submit  --py-files ${SCRIPT_PATH}/../bintrade.zip $*

exit $?
