#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SQLContext
from pyspark import SparkContext
import pytest

from bintrade_tests import sparklib as slib

import eod as eod

local_path = os.path.dirname(__file__)

def test_date():
    deod = [
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2,"adjclose":1.2, "volume": 1000, "date": "2016-01-01"},
        {"symbol": "AAA", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2, "adjclose":1.2, "volume": 1000, "date": "2016-01-02"},
        {"symbol": "MSFT", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2,"adjclose":1.2, "volume": 1000, "date": "2016-01-01"},
        {"symbol": "MSFT", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2, "adjclose":1.2, "volume": 1000, "date": "2016-01-03"}
        ]
    teod = slib.sqlContext.createDataFrame(deod)
    teod.registerAsTable("eod2")

    dindex = [{"ticker": "MSFT"},
              {"ticker": "AAA"},
              {"ticker": "BIDU"}]

    tindex = slib.sqlContext.createDataFrame(dindex)
    tindex.registerAsTable("index_SP500_list")


    deod = [
        {"symbol": "MSFT", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2,"adjclose":1.2, "volume": 1000, "date": "2016-01-01"},
        {"symbol": "MSFT", "open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2, "adjclose":1.2, "volume": 1000, "date": "2016-01-03"}
        ]
    teod = slib.sqlContext.createDataFrame(deod)
    teod.registerAsTable("eod_msft")

    eod.run(slib.sc, slib.sqlContext, isHive=False)


    assert True
