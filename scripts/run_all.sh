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

sh -x ../eod/eod_job.sh || exit $?
sh -x ../post/post_job.sh || exit $?
sh -x ../ta/mat_close_job.sh  || exit $?
sh -x ../ml/ml_cls_pos_job.sh  || exit $?

popd  > /dev/null # return the directory orignal
