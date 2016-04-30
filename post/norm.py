import os
import sys

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/./")
sys.path.append(local_path + "/../")


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
    # re["adjclose"] = ope * re["close"]

    return re


def cal_adj(sc, sql_context, is_hive):
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
        WHERE
            date >= "2000-01-01"
    """)

    return df_eod.rdd.map(lambda x: cal_adj_per(x))


def cal_close_norm_per(x):
    if len(x) == 0:
        return []
    l = []
    x.sort(lambda xx,yy: cmp(xx["date"], yy["date"]),reverse=False)
    per = x[0].copy()
    per["normclose"] = 1.0
    l.append(per.copy())
    for i in range(1, len(x)):
        per = x[i].copy()
        per["normclose"] = l[i-1]["normclose"]/l[i-1]["adjclose"] * per["adjclose"]
        l.append(per.copy())
    return l


def cal_close_norm(rdd_adj):
    return rdd_adj.groupBy(lambda x: x["symbol"]).map(lambda x: (x[0], list(x[1]))) \
           .flatMapValues(lambda x: cal_close_norm_per(x))


def cal_norm_per(x):
    re = x.copy()

    ope = re["normclose"]/re["adjclose"]

    re["normopen"] = ope * re["adjopen"]
    re["normhigh"] = ope * re["adjhigh"]
    re["normlow"] = ope * re["adjlow"]
    # re["normclose"] = ope * re["adjclose"]
    return re


def cal_norm(rdd_close_norm):
    return rdd_close_norm.mapValues(lambda x: cal_norm_per(x)).map(lambda x: x[1])


def save(rdd_norm, sc, sql_context, is_hive):
    rddl_norm = rdd_norm.map(lambda p: (p["date"], p["symbol"], p["open"], p["high"], p["low"], p["close"], p["volume"],
                                                               p["adjopen"], p["adjhigh"], p["adjlow"], p["adjclose"],
                                                               p["normopen"], p["normhigh"], p["normlow"], p["normclose"]
                                        )
                             )
    schema = StructType([
                StructField("date",         StringType(),   True),
                StructField("symbol",       StringType(),   True),
                StructField("open",         FloatType(),    True),
                StructField("high",         FloatType(),    True),
                StructField("low",          FloatType(),    True),
                StructField("close",        FloatType(),    True),
                StructField("volume",       FloatType(),    True),
                StructField("adjopen",      FloatType(),    True),
                StructField("adjhigh",      FloatType(),    True),
                StructField("adjlow",       FloatType(),    True),
                StructField("adjclose",     FloatType(),    True),
                StructField("normopen",     FloatType(),    True),
                StructField("normhigh",     FloatType(),    True),
                StructField("normlow",      FloatType(),    True),
                StructField("normclose",    FloatType(),    True),
            ])
    df_norm = sql_context.createDataFrame(rddl_norm, schema)

    if not is_hive:
        df_norm.registerAsTable("eod_delta")
        return

    sql_context.sql("""
    DROP TABLE IF EXISTS %s
    """ % "eod_norm")


    sql_context.sql("""
        CREATE TABLE IF NOT EXISTS eod_norm(
            date        string,
            symbol      string,
            open        float,
            high        float,
            low         float,
            close       float,
            volume      float,
            adjopen     float,
            adjhigh     float,
            adjlow      float,
            adjclose    float,
            normopen    float,
            normhigh    float,
            normlow     float,
            normclose   float
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        """)

    df_norm.insertInto("eod_norm", overwrite = True)


def main(sc, sql_context, is_hive = True):
    rdd_adj = cal_adj(sc, sql_context, is_hive)
    rdd_close_norm = cal_close_norm(rdd_adj)
    rdd_norm = cal_norm(rdd_close_norm)
    save(rdd_norm, sc, sql_context, is_hive)

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.trade.mat_trade_see", conf=conf)
    sql_context = HiveContext(sc)
    main(sc, sql_context, is_hive = True)
    sc.stop()
