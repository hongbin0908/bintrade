import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier,GBTClassifier,LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.linalg import Vectors, Vector, DenseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees

from tempfile import NamedTemporaryFile
from pyspark.mllib.util import MLUtils
from pyspark import SparkConf, SparkContext, HiveContext,RDD
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import substring


def cal(threshold, start, end, df):
    rdd = df.rdd.filter(lambda x: x.prob > threshold).persist()
    all = rdd.count()
    acc = rdd.filter(lambda x: x.label == 1.0).count()
    print start, end, threshold, acc, all, acc/all
def main(sc, sql_context):
    sqlstr = """
        SELECT
           symbol,
           date1,
           date2,
           date3,
           act_diff,
           prob,
           label
        FROM
           check_pred
    """
    df = sql_context.sql(sqlstr)

    cal(0.6, "2016-03-01", "2016-04-01", df)
    cal(0.6, "2016-02-01", "2016-04-01", df)
    cal(0.6, "2016-01-01", "2016-04-01", df)
    cal(0.6, "2015-12-01", "2016-04-01", df)
    cal(0.6, "2015-11-01", "2016-04-01", df)
    cal(0.6, "2015-10-01", "2016-04-01", df)
    cal(0.6, "2015-09-01", "2016-04-01", df)
    cal(0.6, "2015-08-01", "2016-04-01", df)
    cal(0.6, "2015-07-01", "2016-04-01", df)
    cal(0.6, "2015-06-01", "2016-04-01", df)
    cal(0.6, "2015-05-01", "2016-04-01", df)
    cal(0.6, "2015-04-01", "2016-04-01", df)


if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "8")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "32g")
    sc = SparkContext(appName=__file__, conf=conf)
    sql_context = HiveContext(sc)
    sql_context.setConf("spark.sql.shuffle.partitions", "32")
    sql_context.sql("use fex")
    main(sc, sql_context)
    sc.stop()
