#!/usr/bin/python
# -*- coding:utf-8 -*-  

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


"""
检查每个symbol的股票数据是否全面
"""

if __name__ == "__main__":
    sc = SparkContext(appName="bintrade_check_eod")
    sqlContext = HiveContext(sc)

    sqlContext.sql("use fex")
    dfMSFTOpenDate = sqlContext.sql("""
    SELECT 
        date
    FROM
        eod_msft
    WHERE
        symbol = "MSFT"
        AND date >= '2010-01-01'
    ORDER BY
        date asc
    """)
    dfMSFTOpenDate.cache()
    lenMSFT = dfMSFTOpenDate.count()
    assert(lenMSFT > 100)
    print "len of MSFT:" , lenMSFT

    dfEodShort = sqlContext.sql("""
    SELECT
        symbol,
        count
    FROM
        (
            SELECT
                symbol,
                count(date) as count
            FROM
                eod2
            WHERE
                date >= '2010-01-01'
            GROUP BY
                symbol
        ) a
    WHERE
        a.count < %d 
    """ %(lenMSFT) 
    )

    dfEodShort.cache()
    lenShort = dfEodShort.count()
    print dfEodShort.head()


    print "lenShort:%d" % lenShort
    if lenShort > 35 :
        print "error check failed!!!"
        sys.exit(1)
    sc.stop()
