import os,sys
import json

from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkConf, SparkContext, HiveContext
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
from bintrade_tests.test_lib import *

local_path = os.path.dirname(__file__)


def cal_feature_per(x, window, coach, threshold):
    assert window >= 2
    if len(x) == 0:
        return []
    l = []
    x.sort(lambda xx,yy: cmp(xx.date, yy.date),reverse=False)


    for i in range(0, len(x) - window):
        l_idx = []
        l_feature = []
        idx = 0

        #l_idx.append(x[i].id)
        #l_feature.append(1)
        
        #idx = x[i].id_num

        for j in range(1, window+1):
            l_idx.append(idx)
            #l_feature.append( round(x[i+j].close / x[i+j-1].close,2))
            l_feature.append( int(x[i+j].close / x[i+j-1].close))
            idx += 1
            #l_idx.append(idx)
            #l_feature.append( round(x[i+j].close / x[i+j].open,4))
            #idx += 1
            #l_idx.append(idx)
            #l_feature.append( round(x[i+j].open / x[i+j-1].open,4))
            #idx += 1
        l_idx.append(idx)
        #l_feature.append( round(x[i+window].mat_dual,4))
        l_feature.append( int(x[i+window].mat_dual))
        idx += 1
        #l_idx.append(idx)
        #l_feature.append( round(x[i+window].adx / x[i+j-1].adx,4))
        #idx += 1
        l_idx.append(idx)
        #l_feature.append( round(x[i+window].pdi14 / x[i+window].mdi14,2))
        l_feature.append( int(x[i+window].pdi14 / x[i+window].mdi14))
        idx += 1
        l_idx.append(idx)
        l_feature.append( int(x[i+window].adx/70))
        idx += 1
        


        cls = 0
        is_labeled = 1
        act_diff = -1.0
        if i+window+coach >= len(x):
            cls = -1 # no labels
            date3 = ""
            is_labeled = 0
        elif x[i+window+coach].close / x[i+window].close >= threshold :
            cls = 1
            date3 = x[i+window+coach].date
            act_diff =  x[i+window+coach].close / x[i+window].close
        else:
            cls = 0
            date3 = x[i+window+coach].date
            act_diff =  x[i+window+coach].close / x[i+window].close
        #lp_feature = LabeledPoint( cls, Vectors.sparse(idx, d_feature))
        dx = x[i+window].asDict()
        dx["act_diff"] = act_diff
        dx["is_labeled"] = is_labeled
        dx["date1"] = x[i].date
        dx["date2"] = x[i+window].date
        dx["date3"] = date3
        dx["label"] = cls
        dx["feature_num"] = idx
        dx["fetures_idx"] = str(l_idx)
        dx["features_body"] = str(l_feature)
        l.append(dx)

    return l


def cal_index_per(x):
    assert len(x) > 0
    l = []
    x.sort(lambda xx, yy: cmp(xx.symbol, yy.symbol), reverse = False)
    for i in range(0, len(x)):
        dx = x[i].asDict()
        dx["id"] = i
        l.append(dx)
    return l
def get_lp(sc, sql_context, is_hive):
    df_sym = sql_context.sql("""
    SELECT
        1 AS du,
        symbol
    FROM
        eod_rel
    GROUP BY
        symbol
    ORDER BY
        symbol asc
    """)

    df_sym = df_sym.rdd.groupBy(lambda x: x.du)\
          .map(lambda x: (x[0], list(x[1])))\
          .flatMapValues(lambda x: cal_index_per(x))\
          .map(lambda x: x[1])
    df_sym = sql_context.createDataFrame(df_sym)
    print df_sym.first()
    df_sym = df_sym.withColumn("id_num", df_sym.du*df_sym.count())

    df_close = sql_context.sql("""
    SELECT
        symbol,
        date,
        open  as open,
        close as close
    FROM
        eod_rel
    """)

    df_close = df_close.join(df_sym, [df_close.symbol == df_sym.symbol], 'inner')\
                       .select(df_close.symbol.alias("symbol"),
                               df_sym.id.alias("id"),
                               df_sym.id_num.alias("id_num"),
                               df_close.date.alias("date"),
                               df_close.open.alias("open"),
                               df_close.close.alias("close"))

    print "df_close:", df_close.count()
    print df_close.orderBy(df_close.date.desc()).first()

    df_mat = sql_context.sql("""
    SELECT
        mat1.symbol AS symbol,
        mat2.date AS date,
        mat1.close_mat*1.0/mat2.close_mat AS mat_dual
    FROM
        ta_mat20_close mat1
    INNER JOIN
        ta_mat8_close mat2
    ON
        mat1.symbol = mat2.symbol
        AND mat1.date = mat2.date
    WHERE
        mat1.close_mat > 0
    """)
    print "df_mat:", df_mat.count()
    print df_mat.orderBy(df_mat.date.desc()).first()

    df_adx = sql_context.sql("""
    SELECT
        symbol,
        date,
        pdi14,
        mdi14,
        adx
    FROM
        ta_adx
    WHERE
        adx > 0
    """)

    df_lp = df_close.join(df_mat, [df_close.symbol == df_mat.symbol, df_close.date == df_mat.date], 'inner' )\
                    .join(df_adx, [df_close.symbol == df_adx.symbol, df_close.date == df_adx.date], 'inner' )\
                    .select(df_close.symbol.alias("symbol"),
                            df_close.id.alias("id"),
                            df_close.id_num.alias("id_num"),
                            df_close.date.alias("date"),
                            df_close.open.alias("open"),
                            df_close.close.alias("close"),
                            df_mat.mat_dual.alias("mat_dual"),
                            df_adx.pdi14.alias("pdi14"),
                            df_adx.mdi14.alias("mdi14"),
                            df_adx.adx.alias("adx"))
    print "df_lp:", df_lp.count()

    for each in df_lp.orderBy(df_lp.date.desc()).take(10):
        print each
    return df_lp


def cal_feature(df, window, coach, threshold):
    return df.rdd.groupBy(lambda x: x.symbol).map(lambda x : (x[0], list(x[1])))\
                     .flatMapValues(lambda x: cal_feature_per(x, window, coach, threshold))\
                     .map(lambda x: x[1])


def save(lp, table_name, sql_context, is_hive):
    df = sql_context.createDataFrame(lp)
    print df.first()

    dfToTable(sql_context, df, table_name)
    #rdd = lp.map(lambda p : (p["symbol"],p["act_diff"], p["is_labeled"], p["date1"], p["date2"], p["date3"], p["label"], str(p["lp"])))
    #schema =   StructType([
    #    StructField("symbol",     StringType(), True),
    #    StructField("act_diff",     FloatType(), True),
    #    StructField("is_labeled",   IntegerType(), True),
    #    StructField("date1",   StringType(), True),
    #    StructField("date2",   StringType(), True),
    #    StructField("date3",   StringType(), True),
    #    StructField("label",   FloatType(), True),
    #    StructField("lp", StringType(), True)
    #])
    #df = sql_context.createDataFrame(rdd, schema)

    #if not is_hive:
    #    df.registerAsTable(table_name)
    #    return

    #sql_context.sql("""
    #    DROP TABLE IF EXISTS %s
    #""" % table_name)

    #sql_context.sql("""
    #    CREATE TABLE IF NOT EXISTS %s(
    #        symbol string,
    #        act_diff float,
    #        is_labeled int,
    #        date1 string,
    #        date2 string,
    #        date3 string,
    #        label float,
    #        lp   string
    #        )
    #        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    #        """ % table_name)

    #df.insertInto(table_name, overwrite = True)


def main(window, coach, sc, sql_context, is_hive = True):
    df =  get_lp(sc, sql_context, is_hive)
    #lp = cal_feature(df, window ,coach, 1.00)
    #save(lp,  "point_label_pos", sql_context, is_hive)

    lp = cal_feature(df, window, coach, 1.00)
    save(lp, "point_label", sql_context, is_hive)


if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "8")
    conf.set("spark.executor.cores", "8")
    conf.set("spark.executor.memory", "16g")
    sc = SparkContext(appName="bintrade.ml.diff_feature", conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql(""" use fex """)
    sql_context.setConf("spark.sql.shuffle.partitions", "64")
    main(60, 5, sc, sql_context, is_hive=True)
    sc.stop()
