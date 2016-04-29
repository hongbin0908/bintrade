
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


def do_check(model, x):
    pred = model.predict(x.features)
    return (x.label, pred)



def get_labeled_points(start, end, table_name, sc, sql_context):
    sqlstr = """
        SELECT
            symbol,
            date1,
            date2,
            date3,
            act_diff,
            lp
        FROM
            %s
        WHERE
            date1 >= '%s'
            AND date3 < '%s'
            AND is_labeled = 1
        ORDER BY
            date1,
            symbol
    """ % (table_name, start, end)
    df = sql_context.sql(sqlstr)


    rdd = df.map(lambda x : (x.symbol, x.date2, x.date3, eval(str(x.lp)))).map(lambda x : (x[0], x[1], x[2], x[3][0], Vectors.dense(x[3][1])))
    return sql_context.createDataFrame(rdd, ["symbol", "date2", "date3", "label", "features"])

def get_labeled_points_check(start, end, table_name, sc, sql_context):
    sqlstr = """
        SELECT
            symbol,
            date1,
            date2,
            date3,
            act_diff,
            lp
        FROM
            %s
        WHERE
            date2 >= '%s'
            AND date2 < '%s'
            AND is_labeled = 1
        ORDER BY
            date1,
            symbol
    """ % (table_name, start, end)
    df = sql_context.sql(sqlstr)


    rdd = df.map(lambda x : {"symbol":x.symbol, "date1":x.date1, "date2":x.date2, "date3":x.date3, "act_diff":x.act_diff, "lp": eval(str(x.lp))})\
            .map(lambda x : (x["symbol"], x["date1"], x["date2"], x["date3"], x["act_diff"], x["lp"][0], Vectors.dense(x["lp"][1])))
    return sql_context.createDataFrame(rdd, ["symbol", "date1", "date2", "date3", "act_diff", "label", "features"])
def get_labeled_points_last(table_name, sc, sql_context):
    df = sql_context.sql("""
        SELECT
            symbol,
            is_labeled,
            date1,
            date2,
            date3,
            act_diff,
            lp
        FROM
            %s
        WHERE
            is_labeled = 0
        ORDER BY
            date2 DESC
    """ % table_name)
    rdd = df.map(lambda x : (x.symbol, x.is_labeled, x.date2, x.date3, eval(str(x.lp)))).map(lambda x : (x[0], x[1], x[2],x[3], Vectors.dense(x[4][1])))
    return sql_context.createDataFrame(rdd, ["symbol", "is_labeled", "date2", "date3", "features"])

def get_labeled_points_cur(cur, table_name, sc, sql_context):
    sqlstr ="""
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
            is_labeled = 1
            AND date2 = "%s"
        ORDER BY
            date2 DESC
    """ % (table_name, cur)
    df = sql_context.sql(sqlstr)
    if df.count() == 0:
        return None
    rdd = df.map(lambda x : (x.symbol, x.is_labeled, x.date2, x.date3, eval(str(x.lp)))).map(lambda x : (x[0], x[1], x[2],x[3],x[4][0], Vectors.dense(x[4][1])))
    return sql_context.createDataFrame(rdd, ["symbol", "is_labeled", "date2", "date3", "label", "features"])

def run(start1, end1, start2, end2, sc, sql_context):
    lp_train= get_labeled_points(start1, end1, "point_label_pos", sc, sql_context)
    print lp_train.first()


    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(lp_train)
    td = labelIndexer.transform(lp_train)
    label2index = {}
    for each in  sorted(set([(i[0], i[1]) for i in td.select(td.label, td.indexedLabel).collect()]),
                key=lambda x: x[0]):
        label2index[int(each[0])] = int(each[1])

    featureIndexer = \
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(lp_train)

    #rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")
    #rf = GBTClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", maxIter = 100)
    #rf = LogisticRegression(maxIter = 100000, labelCol="indexedLabel", featuresCol="indexedFeatures")
    rf = get_model()

    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf])

    model = pipeline.fit(lp_train)
    lp_check = get_labeled_points_check(start2, end2, "point_label", sc, sql_context)
    predictions = model.transform(lp_check)
    val(predictions, label2index)
def main(sc, sql_context):
    sql_context.sql("""
    DROP TABLE IF EXISTS %s
    """ % "check_pred")

    sql_context.sql("""
        CREATE TABLE IF NOT EXISTS check_pred(
            symbol  string,
            date1    string,
            date2    string,
            date3    string,
            act_diff float,
            prob     float,
            indexedLabel float,
            label    float,
            pred     float
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        """)
    run("2011-03-01","2015-04-01","2015-04-01", "2016-04-01", sc, sql_context)
    #run("2012-02-01","2016-02-01","2016-02-01", "2016-03-01", sc, sql_context)
    #run("2012-01-01","2016-01-01","2016-01-01", "2016-02-01", sc, sql_context)
    #run("2011-12-01","2015-12-01","2015-12-01", "2016-01-01", sc, sql_context)
    #run("2011-11-01","2015-11-01","2015-11-01", "2015-12-01", sc, sql_context)
    #run("2011-10-01","2015-10-01","2015-10-01", "2015-11-01", sc, sql_context)
    #run("2011-09-01","2015-09-01","2015-09-01", "2015-10-01", sc, sql_context)
    #run("2011-08-01","2015-08-01","2015-08-01", "2015-09-01", sc, sql_context)
    #run("2011-07-01","2015-07-01","2015-07-01", "2015-08-01", sc, sql_context)
    #run("2011-06-01","2015-06-01","2015-06-01", "2015-07-01", sc, sql_context)
    #run("2011-05-01","2015-05-01","2015-05-01", "2015-06-01", sc, sql_context)
    #run("2011-04-01","2015-04-01","2015-04-01", "2015-05-01", sc, sql_context)
    #lp_pred = get_labeled_points_last("point_label", sc, sql_context)
    #predictions = model.transform(lp_pred)
    #for each in predictions.map(lambda x: (x.symbol, x.date2, x.probability.toArray()[0], x.prediction)).collect():
    #    print each[0], each[1], each[2], each[3]
    #for cur in  get_last_days(30, sql_context):
    #    lp_train = get_labeled_points(start1, cur, "point_label_pos", sc, sql_context)
    #    
    #    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(lp_train)
    #    td = labelIndexer.transform(lp_train)
    #    print sorted(set([(i[0], i[1]) for i in td.select(td.label, td.indexedLabel).collect()]),
    #            key=lambda x: x[0])


    #    featureIndexer = \
    #        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(lp_train)

    #    rf = get_model()
    #    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf])
    #    model = pipeline.fit(lp_train) 


    #    lp_pred = get_labeled_points_cur(cur, "point_label", sc, sql_context)
    #    if lp_pred == None:
    #        print "None"
    #        continue

    #    predictions = model.transform(lp_pred)
    #    print cur
    #    val(predictions)




def get_model():
    from pyspark.ml.classification import RandomForestClassifier,GBTClassifier,LogisticRegression,DecisionTreeClassifier
    return DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

def get_last_days(n, sql_context):
    sqlstr = """
        SELECT
            date
        FROM
            eod_spx
        WHERE
            symbol = "SPX"
        ORDER BY
            date desc
        LIMIT %d
    """ % (n)

    df = sql_context.sql(sqlstr)
    return df.rdd.map(lambda x: x.date).collect()
def val_per(predictions, label2index, threshold):
    per =  predictions.map(lambda x: (x.probability.toArray()[label2index[1]],x.label)).persist()
    acc =per.filter(lambda x: x[0] >= threshold).filter(lambda x: x[1] == 1.0).count()
    all = per.filter(lambda x: x[0] >= threshold).count()
    if all != 0: 
        rate = acc*1.0/all
    else:
        rate = 0.0
    print "postive", threshold, acc, all, rate

def val(predictions, label2index):

    sql_context.createDataFrame(predictions.rdd.map(lambda x: (x.symbol, x.date1, x.date2, x.date3,x.act_diff, float(x.probability.toArray()[label2index[1]]),x.indexedLabel, x.label,x.prediction)), ["symbol", "date1", "date2", "date3", "act_diff", "prob", "indexedLabel", "label", "pred"]).insertInto("check_pred", overwrite = False)
    val_per(predictions, label2index, 0.5)
    val_per(predictions, label2index, 0.6)
    val_per(predictions, label2index, 0.65)
    val_per(predictions, label2index, 0.7)
    val_per(predictions, label2index, 0.8)
    val_per(predictions, label2index, 0.9)
    val_per(predictions, label2index, 0.0)

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
    main(sc, sql_context)
    sc.stop()
