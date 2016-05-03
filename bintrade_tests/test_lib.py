#!/usr/bin/env python
# -*- coding:utf-8 -*-
#@author hongbin@youzan.com
import os, sys
import json
local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")

#import  lib.log as log
from pyspark import SQLContext,HiveContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,DoubleType,LongType,MapType

def get_spark():
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "32g")
    sc = SparkContext(appName="youzan-algrithm", conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql(""" use fex """)
    sql_context.setConf("spark.sql.shuffle.partitions", "64")

    return sc, sql_context

def dfToCsv(df, path):
    f = open(path, "w")
    columns = df.columns
    for each in columns:
        print >>f, "%s," % each,
    print >> f
    for each in df.collect():
        d = each.asDict()
        for name in columns:
            print >> f, "%s," % d[name],
        print >> f

def dfToTable(sql_context, df, tableName, overwrite = True, force = True):
    if overwrite == False:
        force = False
    if force:
        sql_context.sql("""
        DROP TABLE IF EXISTS %s
        """ % tableName)
    sqlstr = "CREATE TABLE if NOT EXISTS %s (\n" % tableName
    for each in df.schema.fields:
        sqlstr +="\t" +  each.name
        if isinstance(each.dataType, StringType):
            sqlstr += "\tstring,\n"
        elif isinstance(each.dataType, FloatType):
            sqlstr += "\tfloat,\n"
        elif isinstance(each.dataType, DoubleType):
            sqlstr += "\tdouble,\n"
        elif isinstance(each.dataType, LongType):
            sqlstr += "\tbigint,\n"
        elif isinstance(each.dataType, IntegerType):
            sqlstr += "\tint,\n"
        elif isinstance(each.dataType, MapType):
            sqlstr += "\tmap<int, double>,\n"
        else:
            print "Unknonw " + each.dataType
            assert False
    sqlstr = sqlstr[: len(sqlstr)-2]
    sqlstr += "\n) stored as orc"
    print sqlstr

    sql_context.sql(sqlstr)
    df.insertInto(tableName, overwrite)



if __name__ == '__main__':
    #log.debug("debug")
    #a = eval("(1,[2,3])")
    #print "xxxxxxx",a[1][0]
    #a = {1: 1.0, 3: 5.5}
    #str_a = str(a)
    #a = eval(str_a)
    #print a[1]

    #print json.loads("""{1:1}""")
    sc = SparkContext("local[1]", appName="bintrade.ml.diff_feature")
    sql_context = HiveContext(sc)
    sql_context.sql(""" use fex """)
    sql_context.setConf("spark.sql.shuffle.partitions", "1")


    ldict = [{"symbol":"AAA", "amap":{1:1.0,2:2.1}, "date":"2010-01-01", "close":1.0}, {"symbol":"AAA","date":"2010-01-01", "close":1.0}]

    df = sql_context.createDataFrame(ldict)
    print df.first()
    sql_context.sql("""use fex""")
    sql_context.sql("""
        DROP TABLE IF EXISTS %s
    """ % 'test_eod_AAA')
    #sql_context.sql("create table test_eod_AAA (symbol string, date string, close float) stored as orc")
    #df.write.format("orc").insertInto("test_eod_AAA",True)
    dfToTable(sql_context, df, "test_eod_AAA", overwrite = True)

