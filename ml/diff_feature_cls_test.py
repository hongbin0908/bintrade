#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com

from bintrade_tests.test_lib import *
import ml.diff_feature_cls as feature


def gen_test_data():
    sc, sql_context = get_spark()
    df = sql_context.sql("""
        SELECT
            *
        FROM
            ta_merge
        WHERE
            date >= '2015-01-01'
            AND date < '2015-02-01'
            AND symbol in ("AAA", "MSFT")
    """)

    print df.count()

    sql_context.sql("use fex_test")
    dfToTable(sql_context, df.repartition(1), "ta_merge" )
    sc.stop()

if __name__ == '__main__':
    #gen_test_data()
    
    sc, sql_context = get_spark_test()
    df =  feature.get_lp(sc, sql_context,True)
    for each in df.orderBy(df.symbol.asc(), df.date.asc()).collect():
        print each.symbol, each.date, each.close
    lp = feature.cal_feature(df, 3, 1, 1.00).cache()

    for each in lp.collect():
        print each["symbol"], each["date1"], each["date2"], each["date3"],each["act_diff"], each["label"], each["features"]
