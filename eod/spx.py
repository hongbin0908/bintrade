#!/usr/bin/python
# -*- coding:utf-8 -*-  
#@author hongbin@youzan.com
import os
import sys
from urllib2 import Request, urlopen, URLError, HTTPError

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/./")
sys.path.append(local_path + "/../lib")

"""
Download SPX data
"""

def create_table(sc, sqlContext, isHive = True):
    if not isHive:
        sc.emptyRDD()
        sqlContext.createDataFrame(sc.emptyRDD(), schema = StructType([
                                StructField("date", StringType(), True),
                                StructField("symbol", IntegerType(), True),
                                StructField("open",   FloatType(), True),
                                StructField("high",   FloatType(), True),
                                StructField("low",   FloatType(), True),
                                StructField("close",   FloatType(), True),
                                StructField("volume",   FloatType(), True),
                                StructField("adjclose",   FloatType(), True)])).registerAsTable("eod_spx")
        return

    sqlContext.sql("""
        CREATE TABLE IF NOT EXISTS eod_spx(
            date string,
            symbol string,
            open float,
            high float,
            low float,
            close float,
            volume float,
            adjclose float
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    """)

def getLastDate(sqlContext):
    # {{{ get the latest date
    xx = sqlContext.sql("""
        SELECT
            date
        FROM
            eod_spx
        ORDER BY
            date desc
        LIMIT
            1
    """)
    strLastDate = xx.collect()[0][0]
    return strLastDate


def getCurMsCount(sqlContext):
    xx = sqlContext.sql("""
        SELECT count(*) as count from eod_spx
    """)
    count = xx.collect()[0][0]
    return count

def retrivalNewestMsDate():
    strUrl = "http://chart.finance.yahoo.com/table.csv?s=%s&g=d&ignore=.csv"%("^GSPC")
    print strUrl
    times = 3
    strResp = ""
    while (times > 0) :
        try:
            print strUrl
            response = urlopen(Request(strUrl), timeout = 10)
            strResp = response.readlines()

        except Exception, e:
            print >> sys.stderr, "Error when get(" + str(times) + ") "
            times -= 1
            continue
        break
    assert(len(strResp) > 0)

    ldEod = []
    if strResp == "":
        return ldEod
    for strEach in strResp[1:]:
        each = strEach.split(",")
        assert(len(each) == 7)
        ldEod.append(
                    {"date":str(each[0]),
                     "symbol":str("SPX"),
                     "open":float(each[1]),
                     "high":float(each[2]),
                     "low":float(each[3]),
                     "close":float(each[4]),
                     "volume":float(each[5]),
                     "adjclose":float(each[6])
                     }
                    )
    return ldEod


def save(sc, sqlContext, ldEod, isHive = True):
    rddEod = sc.parallelize(ldEod).map(lambda p : (str(p["date"]),
                                                       str(p["symbol"]),
                                                       p["open"],
                                                       p["high"],
                                                       p["low"],
                                                       p["close"],
                                                       p["volume"],
                                                       p["adjclose"]))
    dfEod = sqlContext.createDataFrame(rddEod,
                StructType([StructField("date",     StringType(), True),
                            StructField("symbol",   StringType(), True),
                            StructField("open",     FloatType(),  True),
                            StructField("high",     FloatType(), True),
                            StructField("low",      FloatType(), True),
                            StructField("close",    FloatType(), True),
                            StructField("volume",   FloatType(), True),
                            StructField("adjclose", FloatType(), True)
                                                               ]))
    if not isHive:
        dfEod.registerAsTable("eod_spx")
        return
    dfEod.repartition(1).insertInto("eod_spx", True)
    #dfEod.registerTempTable("tmp")
    #sqlContext.sql('INSERT OVERWRITE TABLE eod_spx select date,symbol,open,high,low,close,volume,adjclose from tmp')
    # }}}


def main(sc, sqlContext, isHive = True):
    create_table(sc, sqlContext, isHive)
    #strLastDate = getLastDate(sqlContext)
    countOld = getCurMsCount(sqlContext)
    ldEod = retrivalNewestMsDate()
    assert(len(ldEod) >= countOld)
    save(sc, sqlContext, ldEod, isHive)


if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.eod.spx", conf=conf)
    sqlContext = HiveContext(sc)
    sqlContext.sql("use fex")
    main(sc, sqlContext)
    sc.stop()
