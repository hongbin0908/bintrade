#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SQLContext
from pyspark import SparkContext

import eod.candidate as cand
import pytest

import bintrade_tests.sparklib as slib

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")



deod = [
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2, "volume": 1000, "date": "2016-01-01"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2, "volume": 1000, "date": "2016-01-02"},
        {"symbol": "MSFT", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2, "volume": 1000, "date": "2016-01-01"},
        {"symbol": "MSFT", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2, "volume": 1000, "date": "2016-01-03"}
        ]
teod = slib.sqlContext.createDataFrame(deod)
teod.registerAsTable("eod2")

dindex = [{"ticker": "MSFT"},
              {"ticker": "AAA"}]

tindex = slib.sqlContext.createDataFrame(dindex)
tindex.registerAsTable("index_SP500_list")


deod = [
        {"symbol": "MSFT", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2, "volume": 1000, "date": "2016-01-01"},
        {"symbol": "MSFT", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2, "volume": 1000, "date": "2016-01-03"}
        ]
teod = slib.sqlContext.createDataFrame(deod)
teod.registerAsTable("eod_msft")

def test_getSMinMax():
    df = cand.getSMinMax(slib.sqlContext)

    print df.show()
    assert 2 == df.count()

def test_getMSMinMax():
    (min,max) = cand.getMSMinMax(slib.sqlContext)
    assert "2016-01-01" == min
    assert "2016-01-03" == max
def test_getCandi():
    dfSMinMax = cand.getSMinMax(slib.sqlContext)
    dateMin, dateMax = cand.getMSMinMax(slib.sqlContext)
    dfCand = cand.getCandi(dfSMinMax, dateMin, dateMax)
    assert 1 == dfCand.count()
def test_save():
    "hive sql can not ge unit tested"
    pass