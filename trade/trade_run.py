#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")
sys.path.append(local_path + "/../")
sys.path.append(local_path)

from pyspark import SQLContext, SparkConf, HiveContext
from pyspark import SparkContext

from trade import  mat_trade as mat

def run(sc, sql_context, isHive):
    mat.main(sc, sql_context, isHive = True)

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "16")
    conf.set("spark.executor.cores", "16")
    conf.set("spark.executor.memory", "8g")

    sc = SparkContext(appName="bintrade_candidate", master="yarn-client", conf=conf)
    sqlContext = HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")

    sqlContext.sql("use fex")

    run(sc, sqlContext, isHive=True)