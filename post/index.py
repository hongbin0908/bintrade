import os,sys
local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/./")
sys.path.append(local_path + "/../")

import socket
import httplib
from urllib2 import Request, urlopen, URLError, HTTPError
import json

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.rdd import RDD

#from bintrade_tests import sparklib as slib

def cal(sc, sql_context, is_hive):
    df_idxbin = sql_context.sql("""
        SELECT
            "IDXBIN" AS symbol,
            date AS date,
            sum(normopen)*1.0/count(normopen) AS normopen,
            sum(normlow)*1.0/count(normlow) AS normlow,
            sum(normhigh)*1.0/count(normhigh) AS normhigh,
            sum(normclose)*1.0/count(normclose) AS normclose,
            sum(volume)*1.0/count(volume) AS volume
        FROM
            eod_norm
        GROUP BY
            date
        SORT BY
            date asc
    """)
    return df_idxbin

def cal_rel_close_per(x):
    assert len(x) > 0
    l = []
    x.sort(lambda xx,yy: cmp(xx["date"], yy["date"]), reverse=False)
    per = x[0].copy()
    per["relclose"] = 1.0
    l.append(per.copy())
    for i in range(1, len(x)):
        per = x[i].copy()
        per["relclose"] = x[i]["normclose"] / x[i-1]["normclose"]
        l.append(per.copy())
    return l

def cal_index_close_per(x):
    if len(x) == 0:
        return []
    x.sort(lambda xx,yy: cmp(xx["relclose"], yy["relclose"]), reverse=False)

    if len(x) % 2 == 0:
        index = x[len(x)/2-1]["relclose"] + x[len(x)/2+1]["relclose"]
        index = index/2
    else:
        index = x[(len(x)-1)/2]["relclose"]
    return {"symbol":"IDXBIN2", "date":x[0]["date"], "close":index} 

def cumm_per(x):
    assert len(x) > 0
    l = []
    x.sort(lambda xx,yy: cmp(xx["date"], yy["date"]), reverse=False)
    cur = {"symbol":x[0]["symbol"], "date":x[0]["date"], "close":1.0}
    l.append(cur)
    for i in range(1, len(x)):
        assert x[i]["date"] > x[i-1]["date"]
        cur = {"symbol":x[i]["symbol"], "date":x[i]["date"], "close":l[i-1]["close"]*x[i]["close"]}
        l.append(cur)
    return l

def cal(sc, sql_context, is_hive):
    df_idxbin = sql_context.sql("""
        SELECT
            symbol AS symbol,
            date AS date,
            normopen AS normopen,
            normlow AS normlow,
            normhigh AS normhigh,
            normclose AS normclose,
            volume AS volume
        FROM
            eod_norm
        WHERE
            volume > 0
        SORT BY
            symbol asc,
            date asc
    """)

    return df_idxbin.rdd.map(lambda p: {"symbol": p.symbol, "date":p.date, "normclose":p.normclose})\
            .groupBy(lambda x : x["symbol"])\
            .map(lambda x: (x[0], list(x[1]))) \
            .flatMapValues(lambda x: cal_rel_close_per(x))\
            .map(lambda x: x[1]).groupBy(lambda x: x["date"])\
            .map(lambda x: (x[0], list(x[1])))\
            .mapValues(lambda x: cal_index_close_per(x)) \
            .map(lambda x: x[1])\
            .groupBy(lambda x: x["symbol"])\
            .map(lambda x: (x[0], list(x[1])))\
            .flatMapValues(lambda x: cumm_per(x))\
            .map(lambda x: x[1])

def save(df_idxbin, sc, sql_context, is_hive):
    print df_idxbin.first()
    rddl_idxbin = df_idxbin.map(lambda p: (p["symbol"], p["date"],  p["close"]))
    schema = StructType([
         StructField("symbol",       StringType(),   True),
         StructField("date",         StringType(),   True),
         StructField("close",        FloatType(),    True),
         ])
    df_idxbin = sql_context.createDataFrame(rddl_idxbin, schema)

    if not is_hive:
        df_idxbin.registerAsTable("idx")
        return

    sql_context.sql("""
    DROP TABLE IF EXISTS %s
    """ % "idx")

    sql_context.sql("""
        CREATE TABLE IF NOT EXISTS idx(
            symbol  string,
            date    string,
            close   float
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        """)

    df_idxbin.repartition(1).insertInto("idx", overwrite=True)


def main(sc, sql_context, is_hive = True):
    df_idxbin = cal(sc, sql_context, is_hive)
    save(df_idxbin, sc, sql_context, is_hive)

def test_index():
    main(slib.sc, slib.sql_context, is_hive = True)
def get_context():
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="__file__", conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql("""use fex""")
    sql_context.setConf("spark.sql.shuffle.partitions", "32")
    return sc, sql_context

def get_context_test():
    conf = SparkConf()
    sc = SparkContext('local[1]', conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql("""use fex_test""")
    sql_context.setConf("spark.sql.shuffle.partitions", "1")
    return sc, sql_context

if __name__ == "__main__":
    sc, sql_context = get_context()
    main(sc, sql_context, is_hive=True)
    sc.stop()
