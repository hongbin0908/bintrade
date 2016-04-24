#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")
sys.path.append(local_path + "/../")
sys.path.append(local_path + "./")

from pyspark import SQLContext, HiveContext, SparkConf
from pyspark import SparkContext

import ta.mat_close as mat


def duel(sc, sqlContext, window1, window2):
    assert window1 < window2
    dfMatDuel = sqlContext.sql("""
        SELECT
            ta2.symbol,
            ta2.date,
            ta2.close as close,
            ta1.close_mat as mat1,
            ta2.close_mat as mat2
        FROM
            %s ta2
        LEFT JOIN
            %s ta1
        ON
            ta2.symbol = ta1.symbol
            AND ta2.date = ta1.date
        WHERE
            ta2.close_mat >  0
            AND ta2.date  >= '2012-01-01'
        ORDER BY
            ta2.symbol,
            ta2.date
    """ % (mat.get_table_name(window2), mat.get_table_name(window1)) )
    return dfMatDuel

def op(lOneDuel):
    lOneDuel[0]["cash"] = 10000.0
    lOneDuel[0]["stock"] = 0.0
    lOneDuel[0]["signal"] = 0
    for i in range(1, len(lOneDuel)):
        if lOneDuel[i]["mat1"] > lOneDuel[i]["mat2"]:
            if lOneDuel[i-1]["cash"] > 0:
                lOneDuel[i]["signal"] = 1
                lOneDuel[i]["cash"] = 0.0
                lOneDuel[i]["stock"] = lOneDuel[i-1]["stock"] + lOneDuel[i-1]["cash"]/lOneDuel[i]["close"]
            else:
                lOneDuel[i]["signal"] = 0
                lOneDuel[i]["stock"] = lOneDuel[i-1]["stock"]
                lOneDuel[i]["cash"]  = lOneDuel[i-1]["cash"]
        else:
            if lOneDuel[i-1]["stock"] > 0:
                lOneDuel[i]["signal"] = 2
                lOneDuel[i]["stock"] = 0.0
                lOneDuel[i]["cash"] = lOneDuel[i-1]["cash"] + lOneDuel[i-1]["stock"] * lOneDuel[i]["close"]
            else:
                lOneDuel[i]["signal"] = 0
                lOneDuel[i]["cash"] = lOneDuel[i-1]["cash"]
                lOneDuel[i]["stock"] = lOneDuel[i-1]["stock"]
    return lOneDuel

def cal_net(x):
    for each in x:
        each["net"] = each["cash"] + each["stock"] * each["close"]
    return x

def trade(sc, sqlContext, dfMatDuel):
    return dfMatDuel.rdd.groupBy(lambda x: x.symbol).map(lambda x:(x[0], list(x[1]))) \
            .mapValues(lambda x : [{"symbol":each.symbol, "date":each.date, "close":each.close,"mat1":each.mat1, "mat2":each.mat2, "signal":-1, "stock":-1, "cash":-1} for each in x]) \
            .mapValues(lambda x : op(x)) \
            .mapValues(lambda x : cal_net(x)) \
            .flatMap(lambda x : x[1])
def get_table_name(window1, window2):
    return "trade_mat_duel_" + str(window1) + "_" + str(window2)


def save(sc, sqlContext,dfTrade, window1, window2, isHive = True):
    rddTrade = dfTrade.map(lambda p : (p["symbol"],p["date"],p["close"],p["mat1"],p["mat2"], p["signal"], p["cash"],p["stock"],p["net"]))
    schema =   StructType([
                StructField("symbol",     StringType(), True),
                StructField("date",       StringType(), True),
                StructField("close",      FloatType(), True),
                StructField("mat1",       FloatType(), True),
                StructField("mat2",       FloatType(), True),
                StructField("signal",     IntegerType(), True),
                StructField("cash",       StringType(), True),
                StructField("stock",      FloatType(), True),
                StructField("net",        FloatType(), True)
                ])
    dfTradeFor = sqlContext.createDataFrame(rddTrade, schema)

    if not isHive:
        dfTradeFor.registerAsTable(get_table_name(window1, window2))
        return

    table_name = get_table_name(window1, window2)
    sqlContext.sql("""
    DROP TABLE IF EXISTS %s
    """ % table_name)
    sqlContext.sql("""
        CREATE TABLE IF NOT EXISTS %s(
            symbol string,
            date string,
            close float,
            mat1 float,
            mat2 float,
            signal int,
            cash float,
            stock float,
            net float
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            """ % table_name)
    dfTradeFor.insertInto(get_table_name(window1, window2), overwrite = True)


def summary(sc, sqlContext, window1, window2):
    dfSum = sqlContext.sql("""
        SELECT
            symbol      AS symbol,
            max(net)    AS max,
            min(net)    AS min,
            count(if(net > 10000.0,1,null)) AS more_count,
            count(if(net < 10000.0,1,null)) AS less_count
        FROM
            %s
        GROUP BY
            symbol
    """ % get_table_name(window1, window2))

    dfDetail = sqlContext.sql("""
        SELECT
            symbol,
            date,
            net
        FROM
            %s
        ORDER BY
            symbol,
            date desc
    """ % get_table_name(window1, window2))
    rddNet = dfDetail.rdd.groupBy(lambda x : x.symbol).map(lambda x: (x[0], list(x[1]))).mapValues(lambda x : x[0]) \
                .map(lambda x : {"symbol":x[1].symbol, "date":x[1].date, "net":x[1].net})
    dfNet = sqlContext.createDataFrame(rddNet)
    dfSum = dfSum.join(dfNet, ["symbol"], "inner").\
        select(dfSum.symbol.alias("symbol"), dfSum.max.alias("max"), dfSum.min.alias("min"),\
               dfSum.more_count.alias("more_count"), dfSum.less_count.alias("less_count"), dfNet.date.alias("date"), dfNet.net.alias("net"))
    print dfSum.collect()
    for each in dfSum.collect():
        print "%s\t%0.1f\t%0.1f\t%d\t%d\t%s\t%0.1f" % (each.symbol, each.max, each.min, each.more_count, each.less_count, each.date, each.net)


def main(sc, sqlContext, isHive = True):
    ##
    window1 = 14
    window2 = 200
    dfDuel = duel(sc, sqlContext, window1, window2)
    dfTrade = trade(sc, sqlContext, dfDuel)
    save(sc, sqlContext, dfTrade, window1, window2, isHive)
    summary(sc, sqlContext, window1, window2)

if __name__ == "__main__":

    conf = SparkConf()
    conf.set("spark.executor.instances", "16")
    conf.set("spark.executor.cores", "16")
    conf.set("spark.executor.memory", "8g")

    sc = SparkContext(appName="bintrade.trade.mat_trade", master="yarn-client", conf=conf)
    sqlContext = HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")

    sqlContext.sql("use fex")

    trade(sc, sqlContext, isHive=True)


