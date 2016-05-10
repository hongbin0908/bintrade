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

def get_spark(num =4 , cores =4 , mem = "32g"):
    conf = SparkConf()
    conf.set("spark.executor.instances", "%d"%  num)
    conf.set("spark.executor.cores", "%d" % cores)
    conf.set("spark.executor.memory", "%s" % mem)
    sc = SparkContext(appName="youzan-algrithm", conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql(""" use fex """)
    sql_context.setConf("spark.sql.shuffle.partitions", "16")

    return sc, sql_context

def get_spark_test():
    conf = SparkConf()
    sc = SparkContext("local[4]", appName="youzan-algrithm", conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql(""" use fex_test """)
    sql_context.setConf("spark.sql.shuffle.partitions", "1")

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


def table_type2string(dataType):
    if isinstance(dataType, StringType):
        return "string"
    elif isinstance(dataType, FloatType):
        return "float"
    elif isinstance(dataType, DoubleType):
        return "double"
    elif isinstance(dataType, LongType):
        return "bigint"
    elif isinstance(dataType, IntegerType):
        return "int"
    else:
        print "Unknonw " + dataType
        assert False


def dfToTableWithPar(sql_context, df, tableName, par):
    sqlstr = "CREATE TABLE if NOT EXISTS %s (\n" % tableName
    selectstr = ""
    for each in df.schema.fields:
        sqlstr +="\t" +  each.name
        sqlstr += "\t"+table_type2string(each.dataType)+",\n"
        selectstr += each.name + ", "
    sqlstr = sqlstr[: len(sqlstr)-2]
    sqlstr += "\n) "
    sqlstr += "partitioned by(par string)"
    sqlstr += "stored as orc"
    print sqlstr

    sql_context.sql(sqlstr)
    df.registerTempTable("tmp" + tableName)
    selectstr = selectstr[0:len(selectstr)-2]
    sqlstr = """
    INSERT OVERWRITE table %s partition(par='%s')
    SELECT
        %s
    from
        tmp%s
    """ % (tableName, par, selectstr, tableName)
    print sqlstr
    sql_context.sql(sqlstr)



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
        sqlstr += "\t"+table_type2string(each.dataType)+",\n"
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
    sql_context.sql(""" use fex_test """)
    sql_context.setConf("spark.sql.shuffle.partitions", "1")


    ldict = [{"symbol":"AAA", "date":"2010-01-01", "close":1.0}, {"symbol":"AAA","date":"2010-01-01", "close":1.0}]

    df = sql_context.createDataFrame(ldict)
    dfToTableWithPar(sql_context, df,  "test_eod_AAA")

