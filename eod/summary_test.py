#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SQLContext
from pyspark import SparkContext

import eod.summary as summary

local_path = os.path.dirname(__file__)

import bintrade_tests.sparklib as slib


deod = [
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2,"adjclose":1.2, "volume": 1000, "date": "2016-01-01"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2,"adjclose":1.2, "volume": 1000, "date": "2016-01-02"},
        {"symbol": "MSFT", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2, "adjclose":1.2, "volume": 1000, "date": "2016-01-01"},
        {"symbol": "MSFT", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2, "adjclose":1.2, "volume": 1000, "date": "2016-01-03"}
        ]
teod = slib.sqlContext.createDataFrame(deod)
teod.registerAsTable("eod2")

def test_summary():
    summary.main(slib.sc, slib.sqlContext, isHive = False)

if __name__ == "__main__":
    test_summary()
