#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")
sys.path.append(local_path + "/../")

from pyspark import SQLContext, HiveContext
from pyspark import SparkContext

import ta

if __name__ == "__main__":
    sc = SparkContext(appName="bintrade_candidate", master="yarn-client")
    sc.setSystemProperty("spark.driver.memory",     "1g")
    sc.setSystemProperty("spark.executor.memory",   "8g")
    sc.setSystemProperty("spark.executor.instances", "8")
    sc.setSystemProperty("spark.executor.cores",    "4")

    sqlContext = HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")
    sqlContext.sql("use fex")

    ta.run(sc, sqlContext, isHive=True)
