import os,sys

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/./")
sys.path.append(local_path + "/../")

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

from trade import mat_trade


def get(symbol_name, sc, sql_context, is_hive):
    if is_hive:
        sqlContext.sql("use fex")
    df = sqlContext.sql("""
        SELECT
            symbol,
            date,
            close,
            adjclose,
            normclose
        FROM
            eod_norm
        WHERE
            symbol = "%s"
        ORDER BY
            date asc
        """ % (symbol_name)
    )

    return df

def save(symbol_name, df):
    f_out = open(os.path.join(local_path, ("norm_%s_%s.csv" % ("eod_norm", symbol_name))), "w")
    for each in df.collect():
        print >> f_out, each.symbol, "," , each.date, ",", each.close, ",", each.adjclose, ",", each.normclose
    f_out.close()


def main(sc, sql_context, is_hive = True):
    table_name = mat_trade.get_table_name(14, 200)
    symbol_name = "MSFT"
    df = get(symbol_name, sc, sql_context, is_hive)
    save(symbol_name, df)

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.trade.mat_trade_see", conf=conf)
    sqlContext = HiveContext(sc)
    main(sc, sqlContext, is_hive = True)
    sc.stop()
