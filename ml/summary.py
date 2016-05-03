from pyspark import SparkConf, SparkContext, HiveContext
from pyspark.sql.functions import substring
from bintrade_tests.test_lib import *


def cal(threshold, start, end, df):
    rdd = df.rdd.filter(lambda x: x.prob > threshold and x.date2>=start and x.date2<end).persist()
    all = rdd.count()
    acc = rdd.filter(lambda x: x.label == 1.0).count()
    acc2 = rdd.filter(lambda x: x.close2>x.close1).count()
    #acc = rdd.filter(lambda x: x.relclose2>x.relclose1).count()
    #print acc,"\t", round(acc*1.0/all,2),
    #acc = rdd.filter(lambda x: x.close2/x.spxopen2>x.close1/x.spxopen1).count()
    #print acc,"\t", round(acc*1.0/all,2)
    print start,"\t", end,"\t", threshold,"\t", acc,"\t", all, "\t", round(acc*1.0/all,4), "\t", acc2,"\t", round(acc2*1.0/all,4)

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
    """)
    


    cal(0.6, "2015-04-01", "2016-04-01", df)
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
    dfToCsv(df.filter(df.prob>0.6).orderBy(desc("prob"),'symbol','date2'), "check_pred_ext.csv")
if __name__ == "__main__":
    sc, sql_context = get_spark()
    main(sc, sql_context)
    sc.stop()
