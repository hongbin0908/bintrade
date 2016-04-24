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
def cal(sc, sql_context, is_hive):
    if is_hive:
        sql_context.sql(""" use fex """)
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

def save(df_idxbin, sc, sql_context, is_hive):
    if is_hive:
        sql_context.sql("use fex")
    print df_idxbin.first()
    rddl_idxbin = df_idxbin.map(lambda p: (p.symbol, p.date, p.normopen, p.normlow, p.normhigh, p.normclose,p.volume))
    schema = StructType([
                StructField("symbol",           StringType(),   True),
                StructField("date",             StringType(),   True),
                StructField("normopen",         FloatType(),    True),
                StructField("normhigh",         FloatType(),    True),
                StructField("normlow",          FloatType(),    True),
                StructField("normclose",        FloatType(),    True),
                StructField("volume",           FloatType(),    True),
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
            symbol      string,
            date        string,
            normopen    float,
            normhigh    float,
            normlow     float,
            normclose   float,
            volume      float
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        """)

    df_idxbin.repartition(1).insertInto("idx", overwrite=True)


def main(sc, sql_context, is_hive = True):
    df_idxbin = cal(sc, sql_context, is_hive)
    save(df_idxbin, sc, sql_context, is_hive)

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.post.index", conf=conf)
    sql_context = HiveContext(sc)
    main(sc, sql_context, is_hive=True)
    sc.stop()