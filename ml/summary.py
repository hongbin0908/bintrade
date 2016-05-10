from pyspark import SparkConf, SparkContext, HiveContext
from pyspark.sql.functions import substring
from bintrade_tests.test_lib import *
import ml.diff_feature_cls as cls


def cal_(threshold, buffer1, buffer2, start, end, df):
    rdd = df.rdd.filter(lambda x: x.prob >= threshold and x.date2>=start and x.date2<end).cache()
    assert rdd.filter(lambda x: x.label == 1.0 and x.relclose2 < x.relclose1).count() == 0
    all = rdd.count()
    acc = rdd.filter(lambda x: x.act_diff >= buffer1).count()
    acc2 = rdd.filter(lambda x: x.close2*1.0/x.close1>=buffer2).count()

    return (acc,acc2, all)

def cal(threshold, start, end, df):
    acc, acc2, all = cal_(threshold, cls.get_buffer(),1.0, start, end,df)
    acc_w, acc2_w, all_w = cal_(0.0, cls.get_buffer(),1.0, start, end,df)
    if all > 0:
        print "%s\t%f\t%d\t%d\t%f\\%f\t%f\\%f" % (end, threshold, acc, all, acc*1.0/all, acc_w*1.0/all_w, acc2*1.0/all, acc2_w*1.0/all_w)
    else:
        print "%s\t%f\t%d\t%d\t%f\\%f" % (end, threshold, acc, all, 0.0, 0.0)

    

    #acc = rdd.filter(lambda x: x.relclose2>x.relclose1).count()
    #print acc,"\t", round(acc*1.0/all,2),
    #acc = rdd.filter(lambda x: x.close2/x.spxopen2>x.close1/x.spxopen1).count()
    #print acc,"\t", round(acc*1.0/all,2)
    #rdd_whole = df.rdd.filter(lambda x: x.prob > 0.0 and x.date2>=start and x.date2<end).persist()
    #all_whole = rdd_whole.count()
    #acc_whole = rdd_whole.filter(lambda x: x.label == 1.0).count()
    #acc2_whole = rdd_whole.filter(lambda x: x.close2>x.close1).count()
    #print start,"\t", end,"\t", threshold,"\t", \
    #      acc,"      \t", all,       "\t", round(acc*1.0/all,4),             "\t", acc2,      "\t", round(acc2*1.0/all,4),\
    #      acc_whole,"\t", all_whole, "\t", round(acc_whole*1.0/all_whole,4), "\t", acc2_whole,"\t", round(acc2_whole*1.0/all_whole,4)

def main(sc, sql_context):

    df_diff = sql_context.sql("""
        SELECT
            date,
            sum(if(diffclose > 1, 1 , 0)) AS pos,
            sum(if(diffclose <= 1, 1 , 0)) AS neg
        FROM
            ta_diff_5
        GROUP BY
            date
    """)

    df_diff.registerTempTable("tmp_df_diff")

    df = sql_context.sql("""
        SELECT
            p.symbol,
            p.date1,
            p.date2,
            p.date3,
            p.prob,
            p.label,
            p.act_diff,
            r1.close as relclose1,
            r2.close as relclose2,
            spx1.open as spxopen1,
            spx2.open as spxopen2,
            e1.adjclose AS close1,
            e2.adjclose AS close2,
            diff.pos AS num_pos,
            diff.neg AS num_neg
        FROM 
            check_pred p
        LEFT JOIN 
            eod2 e1
        ON
            p.symbol = e1.symbol
            ANd p.date2 = e1.date
        LEFT JOIN
            eod2 e2
        ON
            p.symbol = e2.symbol
            AND p.date3 = e2.date
        LEFT JOIN
            eod_rel r1
        ON
            p.symbol = r1.symbol
            AND p.date2 = r1.date
        LEFT JOIN
            eod_rel r2
        ON
            p.symbol = r2.symbol
            AND p.date3 = r2.date
        LEFT JOIN
            eod_spx spx1
        ON
            p.date2 = spx1.date
        LEFT JOIN
            eod_spx spx2
        ON
            p.date3 = spx2.date
        LEFT JOIN
            tmp_df_diff diff
        ON
            p.date3= diff.date
        WHERE
            p.is_labeled = 1
    """)
    


    cal(0.5, "2015-04-01", "9999-04-01", df)
    cal(0.6, "2015-04-01", "9999-04-01", df)
    cal(0.7, "2015-04-01", "9999-04-01", df)
    cal(0.8, "2015-04-01", "9999-04-01", df)
    cal(0.9, "2015-04-01", "9999-04-01", df)
    cal(1.0, "2015-04-01", "9999-04-01", df)
    cal(0.6, "2016-05-01", "2016-06-01", df)
    cal(0.6, "2016-04-01", "2016-05-01", df)
    cal(0.6, "2016-03-01", "2016-04-01", df)
    cal(0.6, "2016-02-01", "2016-03-01", df)
    cal(0.6, "2016-01-01", "2016-02-01", df)
    cal(0.6, "2015-12-01", "2016-01-01", df)
    cal(0.6, "2015-11-01", "2015-12-01", df)
    cal(0.6, "2015-10-01", "2015-11-01", df)
    cal(0.6, "2015-09-01", "2015-10-01", df)
    cal(0.6, "2015-08-01", "2015-09-01", df)
    cal(0.6, "2015-07-01", "2015-08-01", df)
    cal(0.6, "2015-06-01", "2015-07-01", df)
    cal(0.6, "2015-05-01", "2015-06-01", df)
    cal(0.6, "2015-04-01", "2015-05-01", df)

    dfToTable(sql_context, df, "check_pred_ext")
if __name__ == "__main__":
    sc, sql_context = get_spark()
    main(sc, sql_context)
    sc.stop()
