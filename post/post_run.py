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

from post import norm, index, relative

def main(sc, sql_context, is_hive):
    norm.main(sc, sql_context, is_hive = True)
    index.main(sc, sql_context, is_hive = True)
    relative.main(sc, sql_context, is_hive = True)

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")

    sc = SparkContext(appName="bintrade_candidate", master="yarn-client", conf=conf)
    sqlContext = HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")
    sqlContext.sql("use fex")
    main(sc, sqlContext, is_hive=True)
