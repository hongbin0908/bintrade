
import time

from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees

from tempfile import NamedTemporaryFile
from pyspark.mllib.util import MLUtils
from pyspark import SparkConf, SparkContext, HiveContext,RDD


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
    """ % (table_name, start, end))
    tempFile = NamedTemporaryFile(delete=True)
    print df.rdd.map(lambda x: x.lp).first()
    df.rdd.map(lambda x: x.lp).saveAsTextFile(tempFile.name)
    return tempFile.name

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
    f_train= get_labeled_points("2010-01-01", "2014-12-31", "point_label_pos", sc, sql_context, is_hive)
    print f_train
    lp_train = MLUtils.loadLabeledPoints(sc,f_train)

    f_check = get_labeled_points("2015-01-01", "9999-99-99", "point_label", sc, sql_context, is_hive)
    lp_check = MLUtils.loadLabeledPoints(sc,f_check)

    #model = LogisticRegressionWithLBFGS.train(lp_train, iterations=1e8, corrections= 100, tolerance=1e-8, regParam=0.01)
    #print "xxxxxxxxxxxxxxxx", model._threshold
    #model.clearThreshold()
    model = GradientBoostedTrees.trainClassifier(lp_train, {},
                                                 loss="logLoss", numIterations=100, learningRate=0.05, maxDepth=3,
                                                 maxBins=32)
    preds = model.predict(lp_check.map(lambda x: x.features))
    val(lp_check.map(lambda x: x.label).zip(preds))

    preds = model.predict(lp_train.map(lambda x: x.features))
    val(lp_train.map(lambda x: x.label).zip(preds))

    f_train = get_labeled_points("2010-01-01", "9999-99-99", "point_label_pos", sc, sql_context, is_hive)
    lp_train = MLUtils.loadLabeledPoints(sc,f_train)
    model = LogisticRegressionWithLBFGS.train(lp_train, iterations=1e8, corrections= 100, tolerance=1e-8, regParam=0.01)
    model.clearThreshold()

    lp_pred = get_labeled_points_last("label_point", sc, sql_context, is_hive)

    for each in lp_pred.collect():
        features = Vectors.dense(eval(each.lp)[1])
        print each.date2, each.symbol, model.predict(features)


if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "32g")
    sc = SparkContext(appName="bintrade.post.index", conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql("use fex")
    main(sc, sql_context, is_hive=True)
    sc.stop()
