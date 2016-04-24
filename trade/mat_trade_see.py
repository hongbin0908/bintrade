import os,sys

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/./")
sys.path.append(local_path + "/../")

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.dataframe import Column

from trade import mat_trade

def get(table_name, symbol_name, sc, sqlContext, isHive = True):
    if isHive:
        sqlContext.sql("use fex")
    dfMat = sqlContext.sql("""
        SELECT
            symbol,
            date,
            close,
            mat1,
            mat2,
            signal,
            cash,
            stock,
            net
        FROM
            %s
        WHERE
            symbol = "%s"
        ORDER BY
            date asc
        """ % (table_name, symbol_name)
    )

    return dfMat

def save(table_name, symbol_name, df, sc, sqlContext, isHive = True):
    fout = open(os.path.join(local_path, ("mat_trade_%s_%s.csv" % (table_name, symbol_name))), "w")
    for each in df.collect():
        print >> fout, each.symbol, "," , each.date, ",", each.close, ",", each.mat1, ",", each.mat2, ",", each.signal, \
             "," , each.cash, "," , each.stock, "," , each.net
    fout.close()


def main(sc, sqlContext, isHive = True):
    table_name = mat_trade.get_table_name(14, 200)
    symbol_name = "SPLS"
    df = get(table_name, symbol_name, sc, sqlContext, isHive)
    save(table_name, symbol_name, df, sc, sqlContext, isHive=False)

if __name__ == "__main__":
    conf = SparkConf();
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.trade.mat_trade_see", conf = conf)
    sqlContext = HiveContext(sc)
    main(sc, sqlContext)
    sc.stop()
