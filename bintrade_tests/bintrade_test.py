#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com

import os,sys
local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")
sys.path.append(local_path + "/../")
sys.path.append(local_path)

from pyspark import SQLContext, SparkConf, HiveContext
from pyspark import SparkContext

from post import post_run
from ta import mat_close
from ta import adx
from ml import diff_feature_cls as feature
from ml import diff_train_cls_pos_ml as train


def main(sc, sql_context, is_hive):
    #post_run.main(sc, sql_context, is_hive = True)
    #mat_close.main(sc, sql_context, is_hive = True)
    #adx.main(sc, sql_context, is_hive = True)
    #feature.main(10, 1, sc, sql_context, is_hive = True)
    train.main("2010-01-01", "2010-04-30", "2010-05-01", "9999-99-99", sc, sql_context, is_hive=True)
if __name__ == "__main__":
    conf = SparkConf()
    #conf.set("spark.executor.instances", "4")
    #conf.set("spark.executor.cores", "4")
    #conf.set("spark.executor.memory", "8g")

    sc = SparkContext(appName="bintrade_candidate", master="local[2]", conf=conf)
    sqlContext = HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "1")
    sqlContext.sql("use fex_test")
    main(sc, sqlContext, is_hive=True)
