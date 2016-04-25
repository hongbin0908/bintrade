import os,sys

from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkConf, SparkContext, HiveContext
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType

local_path = os.path.dirname(__file__)

def cal_feature_per(x, window, coach, threshold):
    assert window >= 2
    if len(x) == 0:
        return []
    l = []
    x.sort(lambda xx,yy: cmp(xx.date, yy.date),reverse=False)

    for i in range(0, len(x) - window):
        l_feature = []
        for j in range(1, window+1):
            l_feature.append(x[i+j].close / x[i+j-1].close)
            #l_feature.append(x[i+j].open / x[i].open)
            #l_feature.append(x[i+j].mat_dual)

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
        lp_feature = LabeledPoint( cls, Vectors.dense(l_feature))
        l.append({"symbol":x[i+window].symbol, "act_diff": act_diff, "is_labeled":is_labeled, "date1":x[i].date,"date2":x[i+window].date, "date3":date3, "lp": lp_feature})

    return l


def get_lp(sc, sql_context, is_hive):
    df_close = sql_context.sql("""
    SELECT
        symbol,
        date,
        open  as open,
        close as close
    FROM
        eod_rel
    """).repartition(64)

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
    """).repartition(64)
    print "df_mat:", df_mat.count()
    print df_mat.orderBy(df_mat.date.desc()).first()

    df_adx = sql_context.sql("""
    SELECT
        adx.symbol AS symbol,
        adx.date AS date,
        adx.pdi14 AS pid14,
        adx.mdi14 AS mdi14,,
        adx.adx AS adx
    FROM
        ta_adx adx
    WHERE
        adx.adx > 0
    """).repartition(64)

    df_lp = df_close.join(df_mat, [df_close.symbol == df_mat.symbol, df_close.date == df_mat.date], 'inner' )\
                    .join(df_adx, [df_close.symbol == df_adx.symbol, df_close.date == df_adx.date], 'inner' )\
                    .select(df_close.symbol.alias("symbol"),
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
    rdd = lp.map(lambda p : (p["symbol"],p["act_diff"], p["is_labeled"], p["date1"], p["date2"], p["date3"], str(p["lp"])))
    schema =   StructType([
        StructField("symbol",     StringType(), True),
        StructField("act_diff",     FloatType(), True),
        StructField("is_labeled",   IntegerType(), True),
        StructField("date1",   StringType(), True),
        StructField("date2",   StringType(), True),
        StructField("date3",   StringType(), True),
        StructField("lp", StringType(), True)
    ])
    df = sql_context.createDataFrame(rdd, schema)

    if not is_hive:
        df.registerAsTable(table_name)
        return

    sql_context.sql("""
        DROP TABLE IF EXISTS %s
    """ % table_name)

    sql_context.sql("""
        CREATE TABLE IF NOT EXISTS %s(
            symbol string,
            act_diff float,
            is_labeled int,
            date1 string,
            date2 string,
            date3 string,
            lp   string
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            """ % table_name)

    df.repartition(64).insertInto(table_name, overwrite = True)


def main(sc, sql_context, is_hive = True):
    df =  get_lp(sc, sql_context, is_hive)
    lp = cal_feature(df, 60,4, 1.02)
    save(lp,  "point_label_pos", sql_context, is_hive)

    lp = cal_feature(df, 60, 4, 1.00)
    save(lp, "point_label", sql_context, is_hive)


if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "16g")
    sc = SparkContext(appName="bintrade.ml.diff_feature", conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql(""" use fex """)
    main(sc, sql_context, is_hive=True)
    sc.stop()