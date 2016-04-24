import math

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField


def getDfSC(sc, sqlContext):
    dfSC = sqlContext.sql("""

    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close
    FROM
        eod_rel
    """)

    return dfSC

def create_table(sc, sqlContext,isHive):
    if not isHive:
        sqlContext.createDataFrame(sc.emptyRDD(), schema = StructType([
                                StructField("symbol", StringType(), True),
                                StructField("date", StringType(), True),
                                StructField("tr",   FloatType(), True),
                                StructField("atr",   FloatType(), True)
                                ])).registerAsTable("ta_adx")
        return


    sql_context.sql("""DROP TABLE IF EXISTS %s""" % "ta_adx")

    sqlContext.sql("""
        CREATE TABLE IF NOT EXISTS ta_adx (
            symbol string,
            date string,
            tr float,
            pdm1 float,
            mdm1 float,
            tr14 float,
            pdm14 float,
            mdm14 float,
            pdi14 float,
            mdi14 float,
            di14diff float,
            di14sum float,
            dx float,
            adx float
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        """
    )

def cal(sc, sqlContext, dfSC):
    rdd =  dfSC.rdd.groupBy(lambda x:x.symbol).map(lambda x:(x[0], list(x[1])))
    rdd = rdd.mapValues(lambda x : adx_per(x))
    rdd = rdd.flatMap(lambda x: format(x))

    return rdd


def adx_per(x):
    l = []
    length = len(x)
    if length < 10:
        return l
    x.sort(lambda xx,yy:cmp(xx.date, yy.date),reverse=False)

    l.append({"date":x[0].date, "tr":-1, "pdm1":-1, "mdm1":-1})

    for i in range(0,length - 1):
        sum = 0

        if x[i+1].high - x[i+1].low < 0:
            print x[i+1].symbol, x[i+1].date, x[i+1].high, x[i+1].low
            assert False
        tr1 = x[i+1].high - x[i+1].low; assert tr1 >= 0; tr = tr1
        tr2 = math.fabs(x[i+1].high - x[i].close)
        if tr < tr2:
            tr = tr2
        tr3 = math.fabs(x[i+1].low  - x[i].close)
        if tr < tr3:
            tr = tr3

        h2h1 = x[i+1].high - x[i].high
        l1l2 = x[i].low - x[i+1].low
        if h2h1 > l1l2 :
            pdm1 = h2h1
            if pdm1 < 0:
                pdm1 = 0
        else:
            pdm1 = 0
        if l1l2 > h2h1:
            mdm1 = l1l2
            if mdm1 < 0:
                mdm1 = 0
        else:
            mdm1 = 0

        l.append({"date":x[i+1].date, "tr":tr, "pdm1":pdm1, "mdm1":mdm1})

    tr14 = 0
    pdm14 = 0
    mdm14 = 0
    for i in range(0, 14):
        l[i]["tr14"] = -1.0
        l[i]["pdm14"] = -1.0
        l[i]["mdm14"] = -1.0
        l[i]["pdi14"] = -1.0
        l[i]["mdi14"] = -1.0
        l[i]["di14diff"] = -1.0
        l[i]["di14sum"] = -1.0
        l[i]["dx"] = -1.0
        if i == 0:
            continue
        tr14 += l[i]["tr"]
        pdm14 += l[i]["pdm1"]
        mdm14 += l[i]["mdm1"]


    l[14]["tr14"] = tr14 + l[14]["tr"]
    l[14]["pdm14"] = pdm14 + l[14]["pdm1"]
    l[14]["mdm14"] = mdm14 + l[14]["mdm1"]

    for i in range(15, length):
        l[i]["tr14"] = l[i-1]["tr14"] - l[i-1]["tr14"]/14 + l[i]["tr"]
        l[i]["pdm14"] = l[i-1]["pdm14"] - l[i-1]["pdm14"]/14 + l[i]["pdm1"]
        l[i]["mdm14"] = l[i-1]["mdm14"] - l[i-1]["mdm14"]/14 + l[i]["mdm1"]
    for i in range(14, length):
        pdi14 = l[i]["pdm14"]/l[i]["tr14"]*100
        mdi14 = l[i]["mdm14"]/l[i]["tr14"]*100
        di14diff = math.fabs(pdi14 - mdi14)
        di14sum = pdi14 + mdi14
        dx = di14diff/di14sum*100
        l[i]["pdi14"] = pdi14
        l[i]["mdi14"] = mdi14
        l[i]["di14diff"] = di14diff
        l[i]["di14sum"] = di14sum
        l[i]["dx"] = dx
    for i in range(0, 27):
        l[i]["adx"] = -1.0
    for i in range(27, 28):
        adx = 0
        for j in range(i+1-14, i+1):
            adx += l[j]["dx"]
        adx = adx / 14
        l[i]["adx"] = adx
    for i in range(28, length):
        l[i]["adx"] = (l[i-1]["adx"]*13+l[i]["dx"])/14
    return l

def format(x):
    if len(x) < 2:
        return []
    if len(x[1])<3:
        return []
    return [{"symbol":str(x[0]),
             "date":str(v["date"]),
             "tr":round(v["tr"],6),
             "pdm1":round(v["pdm1"],6),
             "mdm1":round(v["mdm1"],6),
             "tr14":round(v["tr14"],6),
             "pdm14":round(v["pdm14"],6),
             "mdm14":round(v["mdm14"],6),
             "pdi14":round(v["pdi14"],6),
             "mdi14":round(v["mdi14"],6),
             "di14diff":round(v["di14diff"],6),
             "di14sum":round(v["di14sum"],6),
             "dx":round(v["dx"],6),
             "adx":round(v["adx"],6),
             } for v in x[1]]


def save(sc, sqlContext, rdd, isHive = True):
    df = sqlContext.createDataFrame(
                rdd.map(lambda p : (p["symbol"],
                                    p["date"],
                                    p["tr"],
                                    p["pdm1"],
                                    p["mdm1"],
                                    p["tr14"],
                                    p["pdm14"],
                                    p["mdm14"],
                                    p["pdi14"],
                                    p["mdi14"],
                                    p["di14diff"],
                                    p["di14sum"],
                                    p["dx"],
                                    p["adx"]
                                    )),
                StructType([
                    StructField("symbol",        StringType(), True),
                    StructField("date",          StringType(), True),
                    StructField("tr",            FloatType(), True),
                    StructField("pdm1",          FloatType(), True),
                    StructField("mdm1",          FloatType(), True),
                    StructField("tr14",          FloatType(), True),
                    StructField("pdm14",         FloatType(), True),
                    StructField("mdm14",         FloatType(), True),
                    StructField("pdi14",         FloatType(), True),
                    StructField("mdi14",         FloatType(), True),
                    StructField("di14diff",      FloatType(), True),
                    StructField("di14sum",       FloatType(), True),
                    StructField("dx",            FloatType(), True),
                    StructField("adx",           FloatType(), True)
                    ]))
    if not isHive:
        df.registerAsTable("ta_adx")
        return
    df.repartition(16).insertInto("ta_adx", overwrite = True)
def main(sc, sqlContext, is_hive = True):
    dfSC = getDfSC(sc, sqlContext)
    create_table(sc, sqlContext, is_hive)
    rddMat = cal(sc, sqlContext, dfSC)
    save(sc, sqlContext, rddMat,is_hive)

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.ta.mat_close", conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql("use fex")
    main(sc, sql_context, is_hive=True)
    sc.stop()