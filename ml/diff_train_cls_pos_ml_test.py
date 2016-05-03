#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SQLContext
from pyspark import SparkContext
from pyspark.sql import DataFrame

import ml.diff_train_cls_pos_ml as ml
import bintrade_tests.sparklib as slib

local_path = os.path.dirname(__file__)


d_lp = [
    {"symbol": "AAA", "date1": "2010-01-01", "date2":"", "date3":"2010-02-01", "is_labeled":1, "lp":"(0.0, [1.0,1.1,1.2,1.1])"},
    {"symbol": "AAA", "date1": "2010-01-02", "date2":"", "date3":"2010-02-02", "is_labeled":1, "lp":"(0.0, [1.0,1.1,1.2,1.1])"},
    {"symbol": "AAA", "date1": "2010-01-03", "date2":"", "date3":"2010-02-03", "is_labeled":1, "lp":"(1.0, [0.0,0.1,0.2,0.1])"},
    {"symbol": "AAA", "date1": "2010-01-04", "date2":"", "date3":"2010-02-04", "is_labeled":1, "lp":"(1.0, [0.0,0.1,0.2,0.1])"},
    {"symbol": "AAA", "date1": "2015-01-01", "date2":"", "date3":"2015-02-01", "is_labeled":1, "lp":"(0.0, [1.0,1.1,1.2,1.1])"},
    {"symbol": "AAA", "date1": "2015-01-02", "date2":"", "date3":"2015-02-02", "is_labeled":1, "lp":"(0.0, [1.0,1.1,1.2,1.1])"},
    {"symbol": "AAA", "date1": "2015-01-03", "date2":"", "date3":"2015-02-03", "is_labeled":1, "lp":"(1.0, [0.0,0.1,0.2,0.1])"},
    {"symbol": "AAA", "date1": "2015-01-04", "date2":"", "date3":"2015-02-04", "is_labeled":1, "lp":"(1.0, [0.0,0.1,0.2,0.1])"},
        ]
df_lp = slib.sqlContext.createDataFrame(d_lp)
df_lp.registerAsTable("point_label_pos")
df_lp.registerAsTable("point_label")

def test_diff_feature():
    ml.main(slib.sc, slib.sqlContext, is_hive=False)
