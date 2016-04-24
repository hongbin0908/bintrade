#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SQLContext
from pyspark import SparkContext

import post.norm as norm

local_path = os.path.dirname(__file__)

import bintrade_tests.sparklib as slib


d_eod = [
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2,"adjclose":12, "volume": 1000, "date": "2016-01-01"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.3,"adjclose":13, "volume": 1000, "date": "2016-01-02"},
        {"symbol": "MSFT", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.4, "adjclose":14, "volume": 1000, "date": "2016-01-01"},
        {"symbol": "MSFT", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.5, "adjclose":15, "volume": 1000, "date": "2016-01-03"}
        ]
df_eod = slib.sqlContext.createDataFrame(d_eod)
df_eod.registerAsTable("eod2")

def test_cal_adj():
    rdd_adj = norm.cal_adj(slib.sc, slib.sqlContext, is_hive = False)
    print rdd_adj.collect()

def test_cal_close_norm():
    rdd_adj = norm.cal_adj(slib.sc, slib.sqlContext, is_hive = False)
    rdd_close_norm = norm.cal_close_norm(rdd_adj)
    print rdd_close_norm.collect()

def test_cal_norm():
    rdd_adj = norm.cal_adj(slib.sc, slib.sqlContext, is_hive = False)
    rdd_close_norm = norm.cal_close_norm(rdd_adj)
    rdd_norm = norm.cal_norm(rdd_close_norm)
    print rdd_norm.collect()