#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SQLContext
from pyspark import SparkContext
from pyspark.sql import DataFrame

import ml.diff_feature_reg as feature

local_path = os.path.dirname(__file__)

import bintrade_tests.sparklib as slib


d_eod = [
     {"symbol": "AAA", "close": 0.1, "date": "2014-12-20"},
     {"symbol": "AAA", "close": 0.2, "date": "2014-12-21"},
     {"symbol": "AAA", "close": 0.3, "date": "2014-12-22"},
     {"symbol": "AAA", "close": 0.4, "date": "2014-12-23"},
     {"symbol": "AAA", "close": 0.5, "date": "2014-12-24"},
     {"symbol": "AAA", "close": 0.6, "date": "2014-12-25"},
     {"symbol": "AAA", "close": 0.7, "date": "2014-12-26"},
     {"symbol": "AAA", "close": 0.8, "date": "2014-12-27"},
     {"symbol": "AAA", "close": 0.9, "date": "2014-12-28"},
     {"symbol": "AAA", "close": 1.0, "date": "2014-12-29"},
     {"symbol": "AAA", "close": 1.1, "date": "2014-12-30"},
     {"symbol": "AAA", "close": 1.2, "date": "2014-12-31"},
     {"symbol": "AAA", "close": 1.3, "date": "2015-01-01"},
     {"symbol": "AAA", "close": 1.4, "date": "2015-01-02"},
     {"symbol": "AAA", "close": 1.5, "date": "2015-01-03"},
     {"symbol": "AAA", "close": 1.6, "date": "2015-01-04"},
     {"symbol": "AAA", "close": 1.7, "date": "2015-01-05"},
     {"symbol": "AAA", "close": 1.8, "date": "2015-01-06"},
     {"symbol": "AAA", "close": 1.9, "date": "2015-01-07"},
     {"symbol": "AAA", "close": 1.4, "date": "2015-01-08"},
     {"symbol": "AAA", "close": 1.4, "date": "2015-01-09"},
        ]
df_eod = slib.sqlContext.createDataFrame(d_eod)
df_eod.registerAsTable("eod_rel")

def test_diff_feature():
    df_train =  feature.get_train(slib.sc, slib.sqlContext, is_hive = False)
    df_check =  feature.get_check(slib.sc, slib.sqlContext, is_hive = False)
    lp_train =  feature.cal_feature(df_train, 5,2)
    lp_check =  feature.cal_feature(df_check, 5,2)

    lp_train_collected = lp_train.collect()
    assert "(1.2,[1.2,1.16666666667,1.14285714286,1.125,1.11111111111])" == str(lp_train_collected[len(lp_train_collected)-1])
    lp_check_collected = lp_check.collect()
    assert "(0.736842105263,[1.07142857143,1.06666666667,1.0625,1.05882352941,1.05555555556])" == str(lp_check_collected[len(lp_check_collected)-1])

