#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SQLContext
from pyspark import SparkContext
from pyspark.sql import DataFrame

import ml.diff_feature_cls as feature

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


def gen_test_data():
    sc, sql_context = get_spark()
    df = sql_context.sql("""
        SELECT
            *
        FROM
            ta_merge
        WHERE
            date >= 2015-01-01
            AND date < 2015-02-01
            AND symbol in ("AAA", "MSFT")
    """)

    sql_context.sql("use fex_test")
    dfToTable(sql_context, df.repartition(1), "ta_merge" )
    sc.stop()

if __name__ == '__main__':
    gen_test_data()
