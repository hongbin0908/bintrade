#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SQLContext
from pyspark import SparkContext

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")

os.environ["SPARK_HOME"] = "C:\spark-1.6.1-bin-hadoop2.6"
sc = SparkContext('local[1]')
sqlContext = SQLContext(sc)
sqlContext.setConf( "spark.sql.shuffle.partitions", "1")

def create_test():
    sqlContext.sql("CREATE TABLE sample_07 (code string,description string,total_emp int,salary int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TextFile")