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

def main(sc, sql_context):
    sqlstr = """
        SELECT
           cp.symbol,
           cp.date1,
           cp.date2,
           cp.date3,
           eoda.close AS close1,
           spxa.open  AS spxopen1, 
           eodb.close AS close2,
           spxb.open  AS spxopen2, 
           cp.act_diff,
           cp.prob,
           cp.label
        FROM
           check_pred cp
        LEFT JOIN
            eod2 eoda
        ON
            cp.symbol = eoda.symbol
            AND cp.date2 = eoda.date
        LEFT JOIN
            eod2 eodb
        ON
            cp.symbol = eodb.symbol
            AND cp.date3 = eodb.date
        LEFT JOIN
            eod_spx spxa
        ON
            cp.date2 = spxa.date
        LEFT JOIN
            eod_spx spxb
        ON
            cp.date3 = spxb.date
        
    """

    df = sql_context.sql(sqlstr)

    
    sql_context.sql("""
        DROP TABLE IF EXISTS %s
    """ % "check_pred_ext")

    sql_context.sql("""
        CREATE TABLE IF NOT EXISTS %s(
           symbol string,
           date1 string,
           date2 string,
           date3 string,
           close1 float,
           spxopen1 float, 
           close2 float,
           spxopen2 float, 
           act_diff float,
           prob float,
           label float
         )
         ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
         """ % "check_pred_ext")

    df.insertInto("check_pred_ext", overwrite = True)


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
