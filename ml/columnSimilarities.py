#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SparkContext

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")
sys.path.append(local_path + "/../")

from pyspark.mllib.linalg import Vector
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import RowMatrix

def main(sc, sqlContext, isHive = True):
    pass

if __name__ == "__main__":
    os.environ["SPARK_HOME"] = "C:\spark-1.6.1-bin-hadoop2.6"
    sc = SparkContext('local[1]')
    rddRows = sc.parallelize(["1 0 2 0 0 1", "0 0 4 2 0 0"])

    rddRows.map(lambda x: Vectors.dense([float(each) for each in str(x).split(" ")]))
    mat = RowMatrix(rddRows)

    simsPerfect = mat.columnSimilarities()



