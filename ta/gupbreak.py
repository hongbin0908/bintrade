from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from bintrade_tests.test_lib import *


def cal(sc, sqlContext, dfSC):
    rdd =  dfSC.rdd.groupBy(lambda x:x.symbol).map(lambda x:(x[0], list(x[1])))\
        .flatMapValues(lambda x: cal_per(x))\
        .map(lambda x: x[1])

    return rdd


def cal_per(x):
    assert len(x) > 40
    x.sort(lambda xx,yy:cmp(xx.date, yy.date),reverse=False)
    
    start = 39
    l = []

    for i in range(start, len(x)-1):
        dx = x[i].asDict()
        isLabel = True
        for j in range(0, 40):
            if dx["high"] < x[i-j].high:
                isLabel = False
                break
        for j in range(0,9):
            if (dx["high"] - dx["low"]) < (x[i-j].high - x[i-j].low):
                isLabel = False
                break
        if x[i+1].high - dx["high"] < 0.01:
            isLabel = False
        if isLabel:
            dx["label"] = 1
            if i+1<len(x): dx["1day"] = int(x[i+1].close / dx["high"])
            if i+2<len(x): dx["2day"] = int(x[i+2].close / dx["high"])
            if i+3<len(x): dx["3day"] = int(x[i+3].close / dx["high"])
            if i+4<len(x): dx["4day"] = int(x[i+4].close / dx["high"])
            if i+5<len(x): dx["5day"] = int(x[i+5].close / dx["high"])
        else:
            dx["1day"] = -1
            dx["2day"] = -1 
            dx["3day"] = -1 
            dx["4day"] = -1 
            dx["5day"] = -1 
            dx["label"] = 0
        l.append(dx)
    return l
def main(sc, sqlContext):
    dfSC = sqlContext.read.table("eod2")
    rddMat = cal(sc, sqlContext, dfSC)
    df = sqlContext.createDataFrame(rddMat)
    dfToTable(sqlContext, df, "ta_gupbreak")
    


if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName=__file__, conf=conf)
    #sc = SparkContext("local[4]", conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql("use fex")
    main(sc, sql_context)
    sc.stop()
