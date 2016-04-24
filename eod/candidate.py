import os,sys
local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/./")
sys.path.append(local_path + "/../")

import socket
import httplib
from urllib2 import Request, urlopen, URLError, HTTPError
import json
import traceback  

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.rdd import RDD






   # +------+----------+----------+
   # |symbol|  max_date|  min_date|
   # +------+----------+----------+
   # |   BBB|2016-01-02|2016-01-01|
   # |   AAA|2016-01-02|2016-01-01|
   # +------+----------+----------+
def getSMinMax(sqlContext):
    df_sym_min_max = sqlContext.sql("""
    SELECT
       i.ticker as symbol,
       max(e.date) as max_date,
       min(e.date) as min_date
    FROM
        index_SP500_list i
    LEFT JOIN
        eod2 e
    ON
        i.ticker = e.symbol
    GROUP BY
        i.ticker
    """)
    return df_sym_min_max

"""
:return (dateMax, dateMin)
"""
def getMSMinMax(sqlContext):
    dfMSFTMaxMin = sqlContext.sql("""
        SELECT
            max(date) as max,
            min(date) as min
        FROM
            eod_msft
        WHERE
            symbol = "MSFT"
        """)
    dateMin = dfMSFTMaxMin.collect()[0].min
    dateMax = dfMSFTMaxMin.collect()[0].max
    return (dateMin,dateMax)


def isValid(dateMin, dateMax, x):
    y = {}
    y["symbol"] = x["symbol"]
    y["valid"] = True
    if x["max_date"] == None:
        y["max_date"] = dateMin
        y["min_date"] = dateMin
        return y
    assert not x["min_date"] is None
    if x["max_date"] == dateMax:
        y["valid"] = False
        return y
    y["max_date"] = x["max_date"]
    y["min_date"] = x["min_date"]
    return y


"""
IN:
symbol,min_date,max_date

OUT:
symbol, min-date, max_date
"""
def getCandi(dfSMinMax, dateMin, dateMax):
     return dfSMinMax.rdd.map(lambda x: (x.symbol,{"symbol":x.symbol, "max_date":x.max_date,"min_date":x.min_date}))\
                             .mapValues(lambda x: isValid(dateMin, dateMax, x)) \
                             .map(lambda x : x[1]) \
                             .filter(lambda x: x["valid"] == True)


def save(sqlContext ,rddcand, isHive = True):

    if not isHive:
        dfCand = sqlContext.createDataFrame(rddcand)
        dfCand.registerAsTable("candidate")
        return
    sqlContext.sql("""CREATE TABLE IF NOT EXISTS candidate(
            symbol string,
            max_date string,
            min_date string
            )
            """)

    rddSMinMax = rddcand.map(lambda p : (p["symbol"],p["max_date"],p["min_date"]))
    dfSMinMax = sqlContext.createDataFrame(rddSMinMax,
            StructType([
                StructField("symbol",   StringType(), True),
                StructField("max_date", StringType(), True),
                StructField("min_date", StringType(), True),
                ]))
    dfSMinMax.repartition(1).insertInto("candidate", True)
    return


def main(sc, sqlContext, isHive = True):
    dfSMinMax = getSMinMax(sqlContext)
    dateMin,dateMax = getMSMinMax(sqlContext)
    rddcand = getCandi(dfSMinMax, dateMin, dateMax)
    save(sqlContext, rddcand, isHive)


if __name__ == "__main__":
    sc = SparkContext(appName="bintrade_candidate")
    sqlContext = HiveContext(sc)
    sqlContext.sql("use fex")
    main(sc, sqlContext)
    sc.stop()
