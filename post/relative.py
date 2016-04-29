import os, sys

from pyspark import SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.rdd import RDD

local_path = os.path.dirname(__file__)

def get_norm(sc, sql_context, is_hive):
    df_norm = sql_context.sql("""
        SELECT
            symbol,
            date,
            adjopen,
            adjhigh,
            adjlow,
            adjclose,
            volume
        FROM
            eod_norm
    """)
    df_norm = sql_context.sql("""
        SELECT
            symbol,
            date,
            normopen as open,
            normhigh as high,
            normlow as low,
            normclose as close,
            volume
        FROM
            eod_norm
    """)
    return df_norm

def get_idx(sc, sql_context, is_hive):
    df_idx = sql_context.sql("""
        SELECT
            symbol,
            date,
            open  AS open,
            high  AS high,
            low   AS low,
            close AS close,
            volume
        FROM
            eod_spx
        WHERE
            symbol = "SPX"
    """)
    #df_idx = sql_context.sql("""
    #    SELECT
    #        symbol,
    #        date,
    #        close AS close
    #    FROM
    #        idx 
    #    WHERE
    #        symbol = "IDXBIN2"
    #""")
    d_idx = {}
    for each in df_idx.collect():
        d_idx[each.date] = {"symbol": each.symbol,
                      "date": each.date,
                      "close": each.open
                      }
    return d_idx


def cal_per(x, d_idx):
    if len(x) == 0:
        return []
    l = []
    x.sort(lambda xx,yy: cmp(xx.date, yy.date),reverse=False)
    for i in range(0, len(x)):
        date_cur = x[i].date
        if date_cur not in d_idx.keys():
            assert False and date_cur
        assert d_idx[date_cur]["close"] > 0.0 and d_idx[date_cur]
        close = x[i].close / d_idx[date_cur]["close"] * 10000

        zi = close / x[i].close
        open = zi * x[i].open
        #open = x[i].jopen / d_idx[date_cur]["open"]*10000
        low  = zi * x[i].low
        high = zi * x[i].high
        l.append({"symbol": x[i].symbol,
                  "date": x[i].date,
                  "open": open,
                  "low": low,
                  "high": high,
                  "close": close,
                  "volume": 0
                  #"volume": x[i].volume/(d_idx[date_cur]["volume"]) * 10000
                  })
    return l


def cal(df_norm, d_idx):
    rdd_rel = df_norm.rdd.filter(lambda x: d_idx.has_key(x.date)).groupBy(lambda x: x.symbol).map(lambda x: (x[0], list(x[1]))) \
           .flatMapValues(lambda x : cal_per(x, d_idx)).map(lambda x: x[1])
    return rdd_rel


def save(rdd_rel, sc, sql_context, is_hive):
    print rdd_rel.first()
    rddl_rel = rdd_rel.map(lambda p: (p["symbol"], p["date"], p["open"], p["high"], p["low"], p["close"],p["volume"]))
    schema = StructType([
                StructField("symbol",       StringType(),   True),
                StructField("date",         StringType(),   True),
                StructField("open",         FloatType(),    True),
                StructField("high",         FloatType(),    True),
                StructField("low",          FloatType(),    True),
                StructField("close",        FloatType(),    True),
                StructField("volume",       FloatType(),    True),
            ])
    df_rel = sql_context.createDataFrame(rddl_rel, schema)

    if not is_hive:
        df_rel.registerAsTable("eod_rel")
        return

    sql_context.sql("""
    DROP TABLE IF EXISTS %s
    """ % "eod_rel")

    sql_context.sql("""
        CREATE TABLE IF NOT EXISTS eod_rel(
            symbol      string,
            date        string,
            open        float,
            high        float,
            low         float,
            close       float,
            volume      float
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        """)

    df_rel.repartition(16).insertInto("eod_rel", overwrite=True)


def main(sc, sql_context, is_hive = True):
    df_norm = get_norm(sc, sql_context, is_hive)
    d_idx = get_idx(sc, sql_context, is_hive)
    rdd_rel = cal(df_norm, d_idx)
    save(rdd_rel, sc, sql_context, is_hive)

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.post.index", conf=conf)
    sql_context = HiveContext(sc)
    main(sc, sql_context, is_hive=True)
    sc.stop()
