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


def get_eod(sc, sql_context, is_hive):
    df_norm = sql_context.sql("""
        SELECT
            symbol,
            date,
            open,
            low,
            high,
            close,
            adjopen,
            adjlow,
            adjhigh,
            adjclose,
            volume
        FROM
            eod_adj
    """)
    return df_norm


def get_idx(sc, sql_context, is_hive):
    df_idx = sql_context.sql("""
        SELECT
            symbol,
            date,
            normopen as open,
            normhigh as high,
            normlow as low,
            normclose as close,
            volume
        FROM
            idx
        WHERE
            symbol = "IDXBIN"
    """)
    d_idx = {}
    for each in df_idx.collect():
        d_idx[each.date] = {"symbol": each.symbol,
                      "date": each.date,
                      "open": each.open,
                      "high": each.high,
                      "low": each.low,
                      "close": each.close,
                      "volume": each.volume}
    return d_idx


def cal_per(x, d_idx):
    if len(x) == 0:
        return []
    l = []
    x.sort(lambda xx,yy: cmp(xx.date, yy.date),reverse=False)
    for i in range(0, len(x)):
        date_cur = x[i].date
        l.append({"symbol": x[i].symbol,
                  "date": x[i].date,
                  "rltopen": x[i].adjopen*1.0/d_idx[date_cur]["open"] * 10000,
                  "rlthigh": x[i].adjhigh*1.0/d_idx[date_cur]["high"] * 10000,
                  "rltlow": x[i].adjlow*1.0/d_idx[date_cur]["low"] * 10000,
                  "rltclose": x[i].adjclose*1.0/d_idx[date_cur]["close"] * 10000,
                  "rltvolume": x[i].volume*1.0/(d_idx[date_cur]["volume"]+0.00001) * 10000
                  })
    return l


def cal(df_eod, d_spx):
    df_eod = df_eod.rdd.filter(lambda x: d_spx.has_key(x.date))
    rdd = df_eod.groupBy(lambda x: x.symbol).map(lambda x: (x[0], list(x[1]))) \
           .flatMapValues(lambda x : cal_per(x, d_spx)).map(lambda x: x[1])
    return rdd


def save(rdd, sc, sql_context, is_hive):
    rddl = rdd.map(lambda p: (p["symbol"], p["date"], p["rltopen"], p["rltlow"], p["rlthigh"], p["rltclose"],p["rltvolume"]))
    schema = StructType([
                StructField("symbol",          StringType(),   True),
                StructField("date",            StringType(),   True),
                StructField("rltopen",         FloatType(),    True),
                StructField("rlthigh",         FloatType(),    True),
                StructField("rltlow",          FloatType(),    True),
                StructField("rltclose",        FloatType(),    True),
                StructField("rltvolume",       FloatType(),    True),
            ])
    df_rel = sql_context.createDataFrame(rddl, schema)

    if not is_hive:
        df_rel.registerAsTable("eod_rlt")
        return

    sql_context.sql("""
    DROP TABLE IF EXISTS %s
    """ % "eod_rlt")

    sql_context.sql("""
        CREATE TABLE IF NOT EXISTS eod_rlt(
            symbol      string,
            date        string,
            rltopen        float,
            rlthigh        float,
            rltlow         float,
            rltclose       float,
            rltvolume      float
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        """)

    df_rel.repartition(16).insertInto("eod_rlt", overwrite=True)


def main(sc, sql_context, is_hive = True):
    df_eod = get_eod(sc, sql_context, is_hive)
    d_spx = get_idx(sc, sql_context, is_hive)
    rdd_rlt = cal(df_eod, d_spx)
    save(rdd_rlt, sc, sql_context, is_hive)

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.post.rlt", conf=conf)
    sql_context = HiveContext(sc)
    main(sc, sql_context, is_hive=True)
    sc.stop()