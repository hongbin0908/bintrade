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

#sh -x ./run_arg.sh ../eod/eod_run.py || exit $?
#sh -x ./run_arg.sh ../post/post_run.py || exit $?
#sh -x ./run_arg.sh ../ta/mat_close.py  || exit $?
#sh -x ./run_arg.sh ../ta/mat_dual.py  || exit $?
#sh -x ./run_arg.sh ../ta/adx.py  || exit $?
#sh -x ./run_arg.sh ../ta/upbreak.py  || exit $?
#sh -x ./run_arg.sh ../ta/gupbreak.py  || exit $?
#sh -x ./run_arg.sh ../ta/ta_merge.py  || exit $?
sh -x ./run_arg.sh ../ml/diff_feature_cls.py  || exit $?
sh -x ./run_arg.sh ../ml/summary.py  || exit $?

popd  > /dev/null # return the directory orignal
