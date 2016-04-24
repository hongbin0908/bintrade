#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")
sys.path.append(local_path + "/../")
sys.path.append(local_path)

from pyspark import SQLContext, SparkConf
from pyspark import HiveContext
from pyspark import SparkContext

from eod import msft,candidate,download,merge,summary,spx

def run(sc, sql_context, is_hive):
    msft.main(sc, sql_context, is_hive)
    candidate.main(sc, sql_context, is_hive)
    download.main(sc, sql_context, is_hive)
    merge.main(sc, sql_context, is_hive)
    spx.main(sc, sql_context, is_hive)
    print "------------------summary--------------------"
    summary.main(sc, sql_context, is_hive)

if __name__ == '__main__':
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores",     "4")
    conf.set("spark.executor.memory",    "32g")

    sc = SparkContext(appName="bintrade_candidate.eod.eod_run", master="yarn-client", conf=conf)
    sqlContext = HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")

    sqlContext.sql("use fex")

    run(sc, sqlContext, is_hive=True)