import os,sys
import json

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier,GBTClassifier,LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.mllib.linalg import Vectors
from bintrade_tests.test_lib import *

local_path = os.path.dirname(__file__)

def get_cur():
    return "2016-05-04"

def cal_feature_per(x, window, coach, threshold):
    assert window >= 2
    assert len(x) > 0
    l = []
    x.sort(lambda xx,yy: cmp(xx.date, yy.date),reverse=False)

    for i in range(0, len(x) - window):
        l_idx = []
        l_feature = []
        idx = 0

        l_idx.append(x[i].id)
        l_feature.append(1)
        idx = x[i].id_num

        for j in range(1, window+1):
            l_idx.append(idx)
            #l_feature.append( round(x[i+j].close / x[i+j-1].close,2))
            l_feature.append( round(x[i+j].close / x[i+j-1].close,2))
            print x[i+j].date
            #l_feature.append( round(x[i+window].close / x[j].close,2))
            idx += 1
            #l_idx.append(idx)
            #l_feature.append( round(x[i+j].close / x[i+j].open,4))
            #idx += 1
            #l_idx.append(idx)
            #l_feature.append( round(x[i+j].open / x[i+j-1].open,4))
        for j in range(2, window+1):
            l_idx.append(idx)
            l_feature.append( round(x[i+j].close / x[i+j-2].close,2))
            idx += 1
        #for j in range(20, window+1):
        #    l_idx.append(idx)
        #    l_feature.append( round(x[i+j].close / x[i+j-20].close,2))
        #    idx += 1
        for j in range(1, window+1):
            l_idx.append(idx)
            l_feature.append( round(x[i+j].spxopen / x[i+j-1].spxopen,2))
            idx += 1
        for j in range(2, window+1):
            l_idx.append(idx)
            l_feature.append( round(x[i+j].spxopen / x[i+j-2].spxopen,2))
            idx += 1
        for j in range(1, window+1):
            l_idx.append(idx)
            l_feature.append( round(x[i+j].spxclose / x[i+j-1].spxclose,2))
            idx += 1
        for j in range(2, window+1):
            l_idx.append(idx)
            l_feature.append( round(x[i+j].spxclose / x[i+j-2].spxclose,2))
            idx += 1
        #for j in range(1, window+1):
        #    l_idx.append(idx)
        #    l_feature.append( round(x[i+j].spxhigh / x[i+j].spxlow,2))
        #    idx += 1
        #for j in range(1, window+1):
        #    l_idx.append(idx)
        #    if x[i+j].adx > 30 and x[i+j].pdi14 > x[i+j].mdi14:
        #        l_feature.append(1.0)
        #    else:
        #        l_feature.append(0.0)
        #    #l_feature.append( round(x[i+j].adx, 2))
        #    idx += 1

            #l_idx.append(idx)
            #l_feature.append( round(x[i+j].pdi14 / x[i+j].mdi14,2))
            #idx += 1
        l_idx.append(idx)
        #l_feature.append( round(x[i+window].mat_dual,4))
        l_feature.append(round(x[i+window].mat_dual,2))
        idx += 1
        l_idx.append(idx)
        #l_feature.append( round(x[i+window].pdi14 / x[i+window].mdi14,2))
        l_feature.append( round(x[i+window].pdi14 / x[i+window].mdi14,2))
        idx += 1
        l_idx.append(idx)
        l_feature.append( int(x[i+window].adx/30))
        idx += 1
        l_idx.append(idx)
        l_feature.append( x[i+window].gupbreak)
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
        del dx["volume"]
        dx["act_diff"] = act_diff
        dx["is_labeled"] = is_labeled
        dx["date1"] = x[i].date
        dx["date2"] = x[i+window].date
        dx["date3"] = date3
        dx["label"] = cls
        #dx["feature_num"] = idx
        #dx["fetures_idx"] = str(l_idx)
        dx["features"] = Vectors.sparse(idx, l_idx, l_feature)
        #dx["features_body"] = str(l_feature)
        l.append(dx)
        print "date2:", dx["date2"]
        print "date3:", dx["date3"]

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
    df_lp = sql_context.read.table("ta_merge")
    return df_lp


def cal_feature(df, window, coach, threshold):
    return df.rdd.groupBy(lambda x: x.symbol)\
             .map(lambda x : (x[0], list(x[1])))\
             .flatMapValues(lambda x: cal_feature_per(x, window, coach, threshold))\
             .map(lambda x: x[1])


def save(lp, table_name, sql_context, is_hive):
    df = sql_context.createDataFrame(lp)
    print df.first()
    dfToTable(sql_context, df, table_name)

def get_labeled_points(start, end, df, sc, sql_context):
    print df.first()
    return df.filter(df.date1 >= start).filter(df.date3<end).filter(df.label != -1)

def get_labeled_points_last(df, sc, sql_context):
    df =  df.filter(df.is_labeled == 0).withColumn("label", df.label*0)
    df.registerTempTable("tmp_df")
    df = df.filter(df.date2 == get_cur())
    return df


def cal_prob(x,label2index):
    dx = x.asDict()
    del dx["features"]
    del dx["indexedFeatures"]
    dx["prob"] = float(dx["probability"].toArray()[label2index[1]])
    del dx["probability"] #= str(dx["probability"].toArray().tolist())
    del dx["rawPrediction"]
    return dx
def cal_prob2(x,label2index):
    dx = {}
    dx["symbol"] = x.symbol
    dx["date1"] = x.date1
    dx["date2"] = x.date2
    dx["date3"] = x.date3
    dx["label"] = x.label
    dx["prediction"] = x.prediction
    dx["prob"] = float(x.probability.toArray()[label2index[1]])

    return dx
def val(predictions, label2index, sql_context):
    df = sql_context.createDataFrame(predictions.rdd.map(lambda x: cal_prob(x, label2index)))
    print df.count()
    dfToTable(sql_context, df, "check_pred", overwrite = False)

def get_model():
    from pyspark.ml.classification import RandomForestClassifier,GBTClassifier,LogisticRegression,DecisionTreeClassifier,NaiveBayes
    #return RandomForestClassifier(numTrees = 4, maxDepth=10, maxBins = 32, labelCol="indexedLabel", featuresCol="indexedFeatures")
    return DecisionTreeClassifier(maxDepth=5, maxBins = 32, labelCol="indexedLabel", featuresCol="indexedFeatures")
    #return LogisticRegression(maxIter = 1000000,labelCol="indexedLabel", featuresCol="indexedFeatures")


def run(start1, end1, start2, end2, df, sc, sql_context, fout):
    lp_data= get_labeled_points(start1, end2, df, sc, sql_context)
    print lp_data.count()
    lp_data_cur= get_labeled_points_last(df, sc, sql_context)
    lp_data = lp_data.unionAll(lp_data_cur).cache()
    print lp_data.count()

    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(lp_data)
    td = labelIndexer.transform(lp_data)
    label2index = {}
    for each in  sorted(set([(i[0], i[1]) for i in td.select(td.label, td.indexedLabel).distinct().collect()]),
                key=lambda x: x[0]):
        label2index[int(each[0])] = int(each[1])
    print label2index

    featureIndexer = \
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(lp_data)

    rf = get_model()

    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf])

    lp_train = lp_data.filter(lp_data.date3<end1).filter(lp_data.is_labeled == 1)
    model = pipeline.fit(lp_train)
    #lp_check = get_labeled_points(start2, end2, "point_label", sc, sql_context)
    lp_check = lp_data.filter(lp_data.date2>start2).filter(lp_data.is_labeled == 1)
    predictions = model.transform(lp_check)
    val(predictions, label2index, sql_context)

    lp_cur = lp_data.filter(lp_data.is_labeled==0)
    predictions = model.transform(lp_cur)
    predictions = sql_context.createDataFrame(predictions.rdd.map(lambda x: cal_prob2(x, label2index)))
    predictions = predictions.sort(predictions.prob.desc())
    dfToCsv(predictions, fout)

    for each in predictions.take(10):
        print each
def main(window, coach, sc, sql_context, is_hive = True):
    df =  get_lp(sc, sql_context, is_hive)
    print df.count()
    lp = cal_feature(df, window, coach, 1.00).cache()
    print lp.count()
    lp = sql_context.createDataFrame(lp)
    sql_context.sql("""
    DROP TABLE IF EXISTS %s
    """ % "check_pred")
    run("2000-10-01","2015-10-01","2015-10-01", "2016-04-01",lp, sc, sql_context, "pred_%s.1.csv" % get_cur())

    run("2000-04-01","2015-04-01","2015-04-01", "2016-10-01",lp, sc, sql_context, "pred_%s.2.csv" % get_cur())


if __name__ == "__main__":
    sc, sql_context = get_spark()
    main(7, 1, sc, sql_context, is_hive=True)
    sc.stop()
