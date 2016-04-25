
import time

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.linalg import Vectors, Vector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees

from tempfile import NamedTemporaryFile
from pyspark.mllib.util import MLUtils
from pyspark import SparkConf, SparkContext, HiveContext,RDD
from pyspark.sql.types import StructType, StructField, StringType, FloatType


def do_check(model, x):
    pred = model.predict(x.features)
    return (x.label, pred)


def val(labels_and_preds):
    acc = 0
    all = 0
    for each in labels_and_preds.filter(lambda x: x[1] > 0.50).collect():
        if each[0] == 1.0:
            acc += 1
        all += 1
    postive65_acc = acc*1.0/all
    print "postive50:", acc, all, postive65_acc

    acc = 0
    all = 0
    for each in labels_and_preds.filter(lambda x: x[1] < 0.50).collect():
        if each[0] == 0.0:
            acc += 1
        all += 1
    negtive35_acc = acc * 1.0 / all
    print "negtive50:", acc, all, negtive35_acc


    acc = labels_and_preds.filter(lambda x:  x[1] > 0.5 and x[0] == 1).count()
    all = labels_and_preds.filter(lambda x : x[1] > 0.5).count()
    postive_acc = acc * 1.0 / all

    acc = labels_and_preds.filter(lambda x:  x[1] <= 0.5 and x[0] == 0).count()
    all = labels_and_preds.filter(lambda x : x[1] <= 0.5).count()
    negtive_acc = acc * 1.0 / all

    postive_rand_acc = labels_and_preds.filter(lambda x: x[0] == 1).count() * 1.0 / labels_and_preds.count()
    negtive_rand_acc = labels_and_preds.filter(lambda x: x[0] == 0).count() * 1.0 / labels_and_preds.count()

    print postive_acc, negtive_acc, postive_rand_acc, negtive_rand_acc



def get_labeled_points(start, end, table_name, sc, sql_context, is_hive):
    df = sql_context.sql("""
        SELECT
            symbol,
            date1,
            date3,
            lp
        FROM
            %s
        WHERE
            date1 >= '%s'
            AND date3 <= '%s'
            AND is_labeled = 1
        ORDER BY
            date1,
            symbol
    """ % (table_name, start, end))


    rdd = df.map(lambda x : x.lp).map(lambda x : (x[0], Vectors.dense(eval(x[1]))))
    return sql_context.createDataFrame(rdd, ["label", "features"])

def get_labeled_points_last(table_name, sc, sql_context, is_hive):
    df = sql_context.sql("""
        SELECT
            symbol,
            is_labeled,
            date1,
            date2,
            date3,
            lp
        FROM
            %s
        WHERE
            is_labeled = 0
        ORDER BY
            date2 DESC
    """ % table_name)
    return df


def main(sc, sql_context, is_hive = True):
    lp_train= get_labeled_points("2010-01-01", "2014-12-31", "point_label_pos", sc, sql_context, is_hive)
    print lp_train.first()

    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(lp_train)

    featureIndexer = \
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(lp_train)

    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")



if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "8")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "32g")
    sc = SparkContext(appName="bintrade.post.index", conf=conf)
    sc.setCheckpointDir("checkpoint/")
    sql_context = HiveContext(sc)
    sql_context.setConf("spark.sql.shuffle.partitions", "32")
    sql_context.sql("use fex")
    main(sc, sql_context, is_hive=True)
    sc.stop()
