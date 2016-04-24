import os,sys
from tempfile import NamedTemporaryFile

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/./")
sys.path.append(local_path + "/../")

from pyspark import SparkConf, SparkContext, HiveContext


def cal_feature_per(x, window, coach):
    assert window >= 2
    if len(x) == 0:
        return []
    l = []
    x.sort(lambda xx,yy: cmp(xx.date, yy.date),reverse=False)

    for i in range(0, len(x) - window-coach):
        l_feature = []
        for j in range(1, window+1):
            l_feature.append(x[i+j].close / x[i+j-1].close)
        lp_feature = LabeledPoint(x[i+window+coach].close/x[i+window].close, Vectors.dense(l_feature))
        l.append(lp_feature)
    return l


def get_train(sc, sql_context, is_hive):
    df_train = sql_context.sql("""
    SELECT
        symbol,
        date,
        close
    FROM
        eod_rel
    WHERE date <= "2014-12-31"
    """)
    return df_train


def get_check(sc, sql_context, is_hive):
    df_check = sql_context.sql("""
    SELECT
        symbol,
        date,
        close
    FROM
        eod_rel
    WHERE date > "2014-12-31"
    """)
    return df_check


def cal_feature(df, window, coach):
    return df.rdd.groupBy(lambda x: x.symbol).map(lambda x : (x[0], list(x[1])))\
                     .flatMapValues(lambda x: cal_feature_per(x, window, coach))\
                     .map(lambda x: x[1])


def save(lp,path, sc):
    lp.saveAsTextFile(path)


def main(sc, sql_context, is_hive = True):
    df_train =  get_train(sc, sql_context, is_hive)
    df_check = get_check(sc, sql_context, is_hive)
    lp_train = cal_feature(df_train, 180,30)
    lp_check = cal_feature(df_check, 180,30)
    os.system("""
    source ~/.bashrc; hadoop fs -rm -r bintrade.ml.diff.label_point.train; hadoop fs -rm -r bintrade.ml.diff.label_point.check
    """)
    save(lp_train, "bintrade.ml.diff.label_point.train", sc)
    save(lp_check, "bintrade.ml.diff.label_point.check", sc)


if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.ml.diff_feature", conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql(""" use fex """)
    main(sc, sql_context, is_hive=True)
    sc.stop()
