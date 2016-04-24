import os,sys
from tempfile import NamedTemporaryFile

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from pyspark import SparkConf, SparkContext, HiveContext

local_path = os.path.dirname(__file__)


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
        cls = 0
        if x[i+window+coach].close > x[i+window].close :
            cls = 1
        lp_feature = LabeledPoint( cls, Vectors.dense(l_feature))
        l.append(lp_feature)
    return l


def get_train(sc, sql_context, is_hive):
    df_train = sql_context.sql("""
    SELECT
        symbol,
        date,
        mat1.close_mat*1.0/mat2.close_mat
    FROM
        ta_mat20_close mat1
    INNER JOIN
        ta_mat8_close mat2
    ON
        mat1.date = mat2.date
    WHERE
        mat.data <= 2014-12-31
    """)
    return df_train


def get_check(sc, sql_context, is_hive):
    df_check = sql_context.sql("""
    SELECT
        symbol,
        date,
        mat1.close_mat*1.0/mat2.close_mat
    FROM
        ta_mat20_close mat1
    INNER JOIN
        ta_mat8_close mat2
    ON
        mat1.date = mat2.date
    WHERE
        mat.data > 2014-12-31
    """)
    return df_check


def cal_feature(df, window, coach):
    return df.rdd.groupBy(lambda x: x.symbol).map(lambda x : (x[0], list(x[1]))) \
        .flatMapValues(lambda x: cal_feature_per(x, window, coach)) \
        .map(lambda x: x[1])


def save(lp,path, sc):
    lp.saveAsTextFile(path)


def main(sc, sql_context, is_hive = True):
    df_train =  get_train(sc, sql_context, is_hive)
    df_check = get_check(sc, sql_context, is_hive)
    lp_train = cal_feature(df_train, 60,3)
    lp_check = cal_feature(df_check, 60,3)

    os.system("""
    source ~/.bashrc; hadoop fs -rm -r bintrade.ml.diff.label_point.train.cls; hadoop fs -rm -r bintrade.ml.diff.label_point.check.cls
    """)
    save(lp_train, "bintrade.ml.diff.label_point.train.cls", sc)
    save(lp_check, "bintrade.ml.diff.label_point.check.cls", sc)


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

