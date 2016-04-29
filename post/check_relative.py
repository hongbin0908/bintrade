import os, sys

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.rdd import RDD

local_path = os.path.dirname(__file__)

import post.relative  as rel

def cal_per(x):
    assert len(x) > 0 
    l = []
    x.sort(lambda xx,yy: cmp(xx.date, yy.date), reverse=False)
    cur = {"symbol":x[0].symbol, "date":x[0].date, "close":x[0].close}
    for i in range(1, len(x)):
        cur = {"symbol":x[i].symbol, "date":x[i].date, "close":x[i].close/x[i-1].close}

        l.append(cur)
    return l

def cal_sum_per(x):
    assert len(x) > 0

    pos = 0
    neg = 0
    for each in x:
        if each["close"] >= 1:
            pos += 1
        else:
            neg += 1
    return {"pos":pos, "neg":neg}
        
    
def main(sc, sql_context):
    df = sql_context.sql("""
    SELECT 
        symbol,
        date,
        close
    FROM
        eod_rel 
    """)

    rdd_sum = df.rdd.groupBy(lambda x: x.symbol)\
      .map(lambda x: (x[0], list(x[1])))\
      .flatMapValues(lambda x: cal_per(x))\
      .map(lambda x: x[1])\
      .groupBy(lambda x: x["date"])\
      .map(lambda x: (x[0], list(x[1])))\
      .mapValues(lambda x: cal_sum_per(x))\
      .map(lambda x: x[1])

    for each in rdd_sum.collect():
        print each



if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.post.index", conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql("use fex")
    main(sc, sql_context)
    sc.stop()
