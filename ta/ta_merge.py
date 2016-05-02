from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from bintrade_tests.test_lib import *

def cal_id_per(x):
    assert len(x) > 0
    l = []
    x.sort(lambda xx, yy: cmp(xx.symbol, yy.symbol), reverse = False)
    for i in range(0, len(x)):
        dx = x[i].asDict()
        dx["id"] = i
        l.append(dx)
    return l
def main(sc, sqlContext):
    df_eod = sql_context.read.table("eod_rel")
    df_sym = sql_context.sql("""
    SELECT
         1 AS du,
        symbol
    FROM
        eod_rel
    GROUP BY
        symbol
    ORDER BY
        symbol asc
    """)
    df_sym = df_sym.rdd.groupBy(lambda x: x.du)\
          .map(lambda x: (x[0], list(x[1])))\
          .flatMapValues(lambda x: cal_id_per(x))\
          .map(lambda x: x[1])
    df_sym = sql_context.createDataFrame(df_sym)
    print df_sym.first()
    df_sym = df_sym.withColumn("id_num", df_sym.du * df_sym.count())

    df_sym.registerTempTable("tmp_sym")

    df = sql_context.sql("""
        SELECT 
            e.*,
            s.id,
            s.id_num,
            d.mat_dual,
            a.adx,
            a.pdi14,
            a.mdi14,
            ub.label AS gupbreak,
            spx.open AS spxopen,
            spx.high AS spxhigh,
            spx.low AS spxlow,
            spx.close AS spxclose,

            spx.volume AS spxVolume
        FROM
            eod_rel e
        LEFT JOIN
            tmp_sym s
        ON
            e.symbol = s.symbol
        LEFT JOIN
            ta_dual_8_20 d
        ON
            e.symbol = d.symbol
            AND e.date = d.date
        LEFT JOIN
            ta_adx a
        ON
            e.symbol = a.symbol
            AND e.date = a.date
        LEFT JOIN
            ta_gupbreak ub
        ON
            e.symbol = ub.symbol
            AND e.date = ub.date
        LEFT JOIN
            eod_spx spx
        ON
            e.date = spx.date
        WHERE
            d.mat_dual > 0
            AND a.adx > 0
            AND ub.label >= 0
            
    """)

    dfToTable(sql_context, df, "ta_merge")

if __name__ == "__main__":
    sc, sql_context = get_spark()
    main(sc, sql_context)
    sc.stop()
