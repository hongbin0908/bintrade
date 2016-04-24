#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SQLContext, HiveContext
from pyspark import SparkContext

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")

def main(sc, sqlContext, isHive = True):
    dfSymMDate = sqlContext.sql("""
        SELECT
            symbol, max(date) as max, count(date) as c
        FROM
            eod2
        GROUP BY
            symbol
        ORDER BY
            symbol
    """)
    for each in dfSymMDate.collect():
        print "%s\t%s\t%d" % (each.symbol, each.max, each.c)
if __name__ == "__main__":
    sc = SparkContext(appName="bintrade_candidate")
    sqlContext = HiveContext(sc)
    sqlContext.sql("use fex")
    main(sc, sqlContext)
    sc.stop()