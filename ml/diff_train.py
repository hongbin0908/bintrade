import os,sys
from tempfile import NamedTemporaryFile

import math
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/./")
sys.path.append(local_path + "/../")

from pyspark.mllib.util import MLUtils
from pyspark import SparkConf, SparkContext, HiveContext


def do_check(model, x):
    pred = model.predict(x.features)
    return (x.label, pred)


def main(sc, sql_context, is_hive = True):
    lp_train = MLUtils.loadLabeledPoints(sc,"bintrade.ml.diff.label_point.train")
    lp_check = MLUtils.loadLabeledPoints(sc,"bintrade.ml.diff.label_point.check")

    model = GradientBoostedTrees.trainRegressor(lp_train, {}, numIterations=50, maxDepth= 10 )

    preds = model.predict(lp_check.map(lambda x: x.features))
    labels_and_preds = lp_check.map(lambda x: x.label).zip(preds).sortBy(lambda x: x[1], ascending=False)

    for each in labels_and_preds.take(100):
        print each

    labels_and_preds = lp_check.map(lambda x: x.label).zip(preds).sortBy(lambda x: x[1], ascending=True)
    for each in labels_and_preds.take(100):
        print each

    mse = labels_and_preds.map(lambda x: math.pow(x[0]-x[1],2)).sum()/labels_and_preds.count()
    print mse
    mse = labels_and_preds.map(lambda x: math.pow(x[0]-1.0,2)).sum()/labels_and_preds.count()
    print mse

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName="bintrade.post.index", conf=conf)
    sql_context = HiveContext(sc)
    main(sc, sql_context, is_hive=True)
    sc.stop()