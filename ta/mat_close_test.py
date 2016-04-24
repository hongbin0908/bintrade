#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SQLContext
from pyspark import SparkContext

import ta.mat_close as mat

local_path = os.path.dirname(__file__)

import bintrade_tests.sparklib as slib



def test_cal_mat():
    dSC = [
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.1,"adjclose":1.2, "volume": 1000, "date": "2016-01-01"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2,"adjclose":1.2, "volume": 1000, "date": "2016-01-02"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.3, "adjclose":1.2, "volume": 1000, "date": "2016-01-03"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.4, "adjclose":1.2, "volume": 1000, "date": "2016-01-04"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.5, "adjclose":1.2, "volume": 1000, "date": "2016-01-05"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.6, "adjclose":1.2, "volume": 1000, "date": "2016-01-06"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.7, "adjclose":1.2, "volume": 1000, "date": "2016-01-07"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.8, "adjclose":1.2, "volume": 1000, "date": "2016-01-08"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.9, "adjclose":1.2, "volume": 1000, "date": "2016-01-09"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 2.0, "adjclose":1.2, "volume": 1000, "date": "2016-01-10"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 2.1, "adjclose":1.2, "volume": 1000, "date": "2016-01-11"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 2.2, "adjclose":1.2, "volume": 1000, "date": "2016-01-12"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 2.3, "adjclose":1.2, "volume": 1000, "date": "2016-01-13"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 2.4, "adjclose":1.2, "volume": 1000, "date": "2016-01-14"}
        ]
    dfSC = slib.sqlContext.createDataFrame(dSC)
    rddMat = mat.cal_mat(slib.sc, slib.sqlContext, dfSC, 7)
    #rddMat = mat.cal_mat_window(slib.sc, slib.sqlContext, dfSC, 7)
    for each in rddMat.collect():
        print "%s\t%s\t%f\t%f" % (each["symbol"], each["date"], each["close"], each["close_mat"])