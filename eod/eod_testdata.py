import os,sys

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/./")
sys.path.append(local_path + "/../")

import socket
import httplib
from urllib2 import Request, urlopen, URLError, HTTPError
import json
import traceback
import math

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.dataframe import Column

def main(sc, sqlContext, isHive = True):
    sqlContext.sql(""" use fex_test """)
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

    sqlContext.sql(""" use fex """)
    df = sqlContext.sql("""
    SELECT
        *
    FROM
        eod2
    WHERE
        symbol = "YHOO"
        AND date >= "2010-01-01"
        AND date <= "2010-06-30"
    """)

    sqlContext.sql(""" use fex_test """)
    df.repartition(1).insertInto("eod2", True)

    sqlContext.sql(""" use fex_test """)

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

    sqlContext.sql(""" use fex """)
    df = sqlContext.sql("""
    SELECT
        *
    FROM
        eod_spx
    WHERE
        symbol = "SPX"
        AND date >= "2010-01-01"
        AND date <= "2010-06-30"
    """)
    sqlContext.sql(""" use fex_test """)
    df.repartition(1).insertInto("eod_spx", True)


if __name__ == "__main__":
    conf = SparkConf();
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName=__file__, conf = conf)
    sqlContext = HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "1")
    main(sc, sqlContext)
    sc.stop()
