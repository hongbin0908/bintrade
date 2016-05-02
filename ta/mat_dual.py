from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from bintrade_tests.test_lib import *

def main(sc, sqlContext):
    df_mat1 = sql_context.read.table("ta_mat8_close")
    df_mat2 = sql_context.read.table("ta_mat20_close")
    df = df_mat1.join(df_mat2, 
                 [df_mat1.symbol == df_mat2.symbol, df_mat1.date == df_mat2.date],"inner")\
           .where(df_mat2.close_mat > 0)\
           .select(
                   df_mat1.symbol,
                   df_mat1.date,
                   df_mat1.close_mat.alias("close_mat1"),
                   df_mat2.close_mat.alias("close_mat2")
                   )

    df = df.withColumn('mat_dual', df.close_mat1*1.0/df.close_mat2)

    dfToTable(sql_context, df, "ta_dual_8_20")

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
    sc = SparkContext(appName=__file__, conf=conf)
    sql_context = HiveContext(sc)
    sql_context.sql("use fex")
    main(sc, sql_context)
    sc.stop()
