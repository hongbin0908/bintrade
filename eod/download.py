import os,sys
import socket
import httplib
from urllib2 import Request, urlopen, URLError, HTTPError
import json
import traceback  
from datetime import datetime, date, timedelta

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.rdd import RDD


def get_eod(symbol):
    times = 3
    while (times > 0) :
        try:
            strDate = symbol.max_date
            dtDate = datetime.strptime(strDate, "%Y-%m-%d") + timedelta(days=1)
            iYear = dtDate.date().year
            iMonth = dtDate.date().month
            iDay = dtDate.date().day
            
            strUrl = "http://chart.finance.yahoo.com/table.csv?s=%s&a=%d&b=%d&c=%d&g=d&ignore=.csv"%(symbol.symbol,iMonth-1,iDay,iYear)
            response = urlopen(Request(strUrl), timeout = 60)
            strResp = response.readlines()
        except Exception, e:
            print >> sys.stderr, "Error(1) when get(" + str(times) + ") " + str(symbol.symbol) 
            times -= 1
            continue
        lEod = []
        try:
            for each in  strResp[1:]:
                tokens = each.strip().split(",")
                lEod.append((tokens[0], symbol.symbol, float(tokens[1])*1.0, float(tokens[2])*1.0,float(tokens[3])*1.0, float(tokens[4])*1.0, float(tokens[5]) * 1.0, float(tokens[6])*1.0))
            print "SUCC when get("+str(times)+") " + symbol.symbol
            return lEod
        except:
            traceback.print_exc()
            print >> sys.stderr, "Error(4) when get(" + str(times) + ") " + str(symbol.symbol) + " " + str(each)
            times -= 1
    print "ERROR"
    return [("0",symbol.symbol, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)]
    

def getDfSMax(sc, sqlContext):
    dfSMax = sqlContext.sql("""
        SELECT
            symbol, max_date
        FROM
            candidate
        ORDER BY
            symbol
    """)
    return dfSMax


def save(sc, sqlContext,rddEod, isHive = True):
    rddEod = rddEod.map(lambda p : (p["date"],p["symbol"],p["open"],p["high"],p["low"],p["close"],p["volume"],p["adjclose"]))
    schema =   StructType([
                StructField("date",     StringType(), True),
                StructField("symbol",   StringType(), True),
                StructField("open",     StringType(), True),
                StructField("high",     StringType(), True),
                StructField("low",      StringType(), True),
                StructField("close",    StringType(), True),
                StructField("volume",   StringType(), True),
                StructField("adjclose", StringType(), True)
                ])
    dfEod = sqlContext.createDataFrame(rddEod, schema)
    dfEod = dfEod.filter(dfEod.date != "0")

    if not isHive:
        dfEod.registerAsTable("eod_delta")
        return

    sqlContext.sql("""
        CREATE TABLE IF NOT EXISTS eod_delta(
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

    dfEod.repartition(16).insertInto("eod_delta", overwrite = True)
def main(sc, sqlContext, isHive = True):
    dfSMax = getDfSMax(sc, sqlContext)
    rddEod = dfSMax.flatMap(get_eod).filter(lambda x : len(x) == 8)
    rddEod = rddEod.map(lambda x: {
                                                    "date":x[0], \
                                                    "symbol":x[1], \
                                                    "open":x[2], \
                                                    "high":x[3], \
                                                    "low":x[4], \
                                                    "close":x[5],\
                                                    "volume":x[6], \
                                                    "adjclose":x[7]})
    count = rddEod.count()
    print "count:%d" % count
    save(sc, sqlContext, rddEod, isHive)

if __name__ == "__main__":
    sc = SparkContext(appName="bintrade_get_eod")
    sqlContext = HiveContext(sc)
    sqlContext.sql("use fex")
    main(sc, sqlContext)
    sc.stop()