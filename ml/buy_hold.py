from bintrade_tests.test_lib import *

local_path = os.path.dirname(__file__)


def get_table_name():
    return "check_pred_buy_and_hold"


def get_lp(sc, sql_context):
    df_lp = sql_context.read.table("ta_merge")
    return df_lp



def cal_feature_per(x, window, coach, threshold):
    assert window >= 2
    assert len(x) > 0
    l = []
    x.sort(lambda xx,yy: cmp(xx.date, yy.date),reverse=False)
    for i in range(0, len(x) - window):
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
        l.append(dx)
    return l

def cal_feature(df, window, coach, threshold):
    return df.rdd.groupBy(lambda x: x.symbol).map(lambda x : (x[0], list(x[1])))\
                     .flatMapValues(lambda x: cal_feature_per(x, window, coach, threshold))\
                     .map(lambda x: x[1])

def get_labeled_points(start, end, df, sc, sql_context):
    return df.filter(df.date1 >= start).filter(df.date3<end).filter(df.label != -1)

def val(predictions, sql_context):
    dfToTable(sql_context, predictions, get_table_name(), overwrite = False)

def run(start1, end1, start2, end2, df, sc, sql_context, fout):
    lp_data= get_labeled_points(start1, end2, df, sc, sql_context)

    lp_train = lp_data.filter(lp_data.date3<end1).filter(lp_data.is_labeled == 1)
    lp_check = lp_data.filter(lp_data.date2>start2).filter(lp_data.is_labeled == 1)
    predictions = lp_check.withColumn("prob", lp_check.label * 0.0 + 1.0)
    val(predictions, sql_context)

def main(window, coach, sc, sql_context):
    df =  get_lp(sc, sql_context)
    lp = cal_feature(df, window, coach, 1.00).persist()
    print lp.count()
    lp = sql_context.createDataFrame(lp)
    sql_context.sql(""" DROP TABLE IF EXISTS %s """ % get_table_name())
    run("2000-10-01","2015-10-01","2015-10-01", "2016-04-01",lp, sc, sql_context, "pred_2016-05-02.1.csv")

    run("2000-04-01","2015-04-01","2015-04-01", "2016-10-01",lp, sc, sql_context, "pred_2016-05-02.2.csv")


if __name__ == "__main__":
    sc, sql_context = get_spark()
    main(60, 4, sc, sql_context)
    sc.stop()
