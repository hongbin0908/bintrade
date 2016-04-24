import os,sys
import socket
import httplib
from urllib2 import Request, urlopen, URLError, HTTPError
import json
import traceback  
from datetime import datetime, date, timedelta

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
import pyspark.sql.functions as func
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.window import Window
from pyspark.rdd import RDD


def get_table_name(window):
    return "ta_mat" + str(window) + "_close"


def getDfSC(sc, sqlContext):
    dfSC = sqlContext.sql("""

    SELECT
        symbol,
        date,
        close
    FROM
        eod_rel
    """)

    return dfSC

def create_mat_table(sc, sqlContext, window, isHive):
    if not isHive:
        sqlContext.createDataFrame(sc.emptyRDD(), schema = StructType([
                                StructField("symbol", StringType(), True),
                                StructField("date", StringType(), True),
                                StructField("close",   FloatType(), True),
                                StructField("close_mat",   FloatType(), True)
                                ])).registerAsTable(get_table_name(window))
        return
    sqlContext.sql("""
        CREATE TABLE IF NOT EXISTS %s (
            symbol string,
            date string,
            close float,
            close_mat float
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        """ % (get_table_name(window))
    )

def cal_mat(sc, sqlContext, dfSC, window):
    rddMat =  dfSC.rdd.groupBy(lambda x:x.symbol).map(lambda x:(x[0], list(x[1])))
    rddMat = rddMat.mapValues(lambda x : f(window, x))
    rddMat = rddMat.flatMap(lambda x: format(x))

    return rddMat

def cal_mat_window(sc, sqlContext, dfSC, window):
    windowSpec = Window.partitionBy("symbol").orderBy("date").rangeBetween(-1 * window+1,1)
    mat = func.avg("close").over(windowSpec)
    dfSC = dfSC.select(dfSC.symbol, dfSC.date, dfSC.close, mat )
    print dfSC.collect()

def f(window, x):
    l = []
    length = len(x)
    if length < window:
        return l
    x.sort(lambda xx,yy:cmp(xx.date, yy.date),reverse=False)
    for i in range(0,window-1):
        l.append((x[i].date, x[i].close, -1.0))
    for i in range(0,length - window + 1):
        sum = 0
        for j in range(i, i+window):
            sum += x[j].close
        l.append((x[i+window-1].date,x[i+window-1].close, sum/window))
    return l


def format(x):
    if len(x) < 2:
        return []
    if len(x[1])<3:
        return []
    return [{"symbol":str(x[0]),"date":str(v[0]),"close":float(v[1]),"close_mat":float(v[2])} for v in x[1]]


def save(sc, sqlContext, rddMat, window, isHive = True):
    dfMat = sqlContext.createDataFrame(
                rddMat.map(lambda p : (p["symbol"],p["date"],p["close"],p["close_mat"])),
                StructType([
                    StructField("symbol",        StringType(), True),
                    StructField("date",          StringType(), True),
                    StructField("close",         FloatType(),  True),
                    StructField("close_mat",     FloatType(),  True)
                    ]))
    if not isHive:
        dfMat.registerAsTable(get_table_name(window))
        return
    dfMat.repartition(16).insertInto(get_table_name(window), overwrite = True)
def main(sc, sqlContext, is_hive = True):
    dfSC = getDfSC(sc, sqlContext)
    for window in (8,20):
        create_mat_table(sc, sqlContext, window, is_hive)

        rddMat = cal_mat(sc, sqlContext, dfSC, window)

        save(sc, sqlContext, rddMat, window, is_hive)

        dfMat = None
        rddMat = None

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.ta.mat_close", conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql("use fex")
    main(sc, sql_context, is_hive=True)
    sc.stop()