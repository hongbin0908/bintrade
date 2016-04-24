import os,sys

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/./")
sys.path.append(local_path + "/../")

import socket
import httplib
from urllib2 import Request, urlopen, URLError, HTTPError
import json
import traceback
import math

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.dataframe import Column

def get_mat(sc, sqlContext, isHive = True):
    if isHive:
        sqlContext.sql("use fex")
    dfMat = sqlContext.sql("""
    SELECT
        mat1.symbol AS symbol,
        mat1.date AS date,
        mat1.close AS close,
        mat1.close_mat AS mat1,
        mat2.close_mat AS mat2
    FROM
        ta_mat8_close mat1
    LEFT JOIN
        ta_mat100_close mat2
    ON
        mat1.symbol = mat2.symbol
        AND mat1.date = mat2.date
    WHERE
        mat1.symbol = "MSFT"
    ORDER BY
        mat1.date asc
    """)

    return dfMat

def save(dfMat, sc, sqlContext, isHive = True):
    fout = open(os.path.join(local_path, "mat.csv"), "w")
    for each in dfMat.collect():
        print >> fout, each.symbol, "," , each.date, ",", each.close, ",", each.mat1, ",", each.mat2
    fout.close()

def main(sc, sqlContext, isHive = True):
    dfMat = get_mat(sc, sqlContext, isHive)
    save(dfMat, sc, sqlContext, isHive=False)

if __name__ == "__main__":
    conf = SparkConf();
    conf.set("spark.executor.instances", "16")
    conf.set("spark.executor.cores", "16")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="st.gmv.gmv", conf = conf)
    sqlContext = HiveContext(sc)
    main(sc, sqlContext)
    sc.stop()
