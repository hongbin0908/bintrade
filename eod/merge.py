import os,sys
import socket
import httplib
from urllib2 import Request, urlopen, URLError, HTTPError
import json
import traceback  
from datetime import datetime, date, timedelta

import pyspark
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.rdd import RDD


def main(sc, sqlContext, isHive = True):

    dfEod = sqlContext.sql("""
        SELECT
            f.date,
            f.symbol,
            f.open,
            f.high,
            f.low,
            f.close,
            f.volume,
            f.adjclose
        FROM
            (
                SELECT
                    o.date,
                    o.symbol,
                    o.open,
                    o.high,
                    o.low,
                    o.close,
                    o.volume,
                    o.adjclose
                FROM
                    eod2 o
                UNION ALL
                SELECT
                    n.date,
                    n.symbol,
                    n.open,
                    n.high,
                    n.low,
                    n.close,
                    n.volume,
                    n.adjclose
                FROM
                    eod_delta n
            ) f
        WHERE
            f.date >= "1900-01-01"
            AND symbol is not null
    """)
    count_dfEod = dfEod.count()
    print "count_dfEod:%d" % count_dfEod
    dfEod = dfEod.dropDuplicates(["date","symbol"])



    if not isHive:
        dfEod.registerAsTable("eod2")
        return
    sqlContext.sql("""
        CREATE TABLE IF NOT EXISTS eod2(
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
    dfEod.insertInto("eod2", True)
    return

if __name__ == "__main__":
    sc = SparkContext(appName="bintrade_merge")
    sqlContext = HiveContext(sc)
    sqlContext.sql("use fex")
    main(sc, sqlContext, isHive = True)
    sc.stop()
