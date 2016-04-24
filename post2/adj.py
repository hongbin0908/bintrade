import os,sys
local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/./")
sys.path.append(local_path + "/../")

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField


def cal_adj_per(x):
    re = dict({})
    re["symbol"] = x.symbol
    re["date"] = x.date
    re["open"] = x.open
    re["high"] = x.high
    re["low"] = x.low
    re["close"] = x.close
    re["volume"] = x.volume
    re["adjclose"] = x.adjclose

    ope = re["adjclose"]/re["close"]

    re["adjopen"] = ope * re["open"]
    re["adjhigh"] = ope * re["high"]
    re["adjlow"] = ope * re["low"]

    return re


def cal_adj(sc, sql_context, is_hive):
    if is_hive:
        sql_context.sql("use fex")
    df_eod = sql_context.sql("""
        SELECT
            symbol,
            date,
            open,
            high,
            low,
            close,
            volume,
            adjclose
        FROM
            eod2
    """)

    return df_eod.rdd.map(lambda x: cal_adj_per(x))


def save(rdd, sc, sql_context, is_hive):
    rddl = rdd.map(lambda p: (p["symbol"],
                               p["date"],
                               p["open"],
                               p["low"],
                               p["high"],
                               p["close"],
                               p["adjopen"],
                               p["adjlow"],
                               p["adjhigh"],
                               p["adjclose"],
                               p["volume"]))
    schema = StructType([
                StructField("symbol",       StringType(),   True),
                StructField("date",         StringType(),   True),
                StructField("open",         FloatType(),    True),
                StructField("high",         FloatType(),    True),
                StructField("low",          FloatType(),    True),
                StructField("close",        FloatType(),    True),
                StructField("adjopen",         FloatType(),    True),
                StructField("adjhigh",         FloatType(),    True),
                StructField("adjlow",          FloatType(),    True),
                StructField("adjclose",        FloatType(),    True),
                StructField("volume",       FloatType(),    True),
            ])
    df_rel = sql_context.createDataFrame(rddl, schema)

    if not is_hive:
        df_rel.registerAsTable("eod_adj")
        return

    sql_context.sql("""
    DROP TABLE IF EXISTS %s
    """ % "eod_adj")

    sql_context.sql("""
        CREATE TABLE IF NOT EXISTS eod_adj(
            symbol      string,
            date        string,
            open        float,
            high        float,
            low         float,
            close       float,
            adjopen     float,
            adjhigh     float,
            adjlow      float,
            adjclose    float,
            volume      float
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        """)

    df_rel.repartition(16).insertInto("eod_adj", overwrite=True)


def main(sc, sql_context, is_hive = True):
    rdd = cal_adj(sc, sql_context, is_hive)
    save(rdd, sc, sql_context, is_hive)

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.post.adj", conf=conf)
    sql_context = HiveContext(sc)
    main(sc, sql_context, is_hive=True)
    sc.stop()