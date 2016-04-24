import os

from pyspark import SparkContext
from pyspark.sql import HiveContext

if __name__ == "__main__":
    sc = SparkContext(appName="sqlIndexSP500List")
    sqlContext = HiveContext(sc)
    sqlContext.sql("use fex")
    symbols = sqlContext.sql("select * from index_SP500_list")
    symbols.registerAsTable("tmp")
    sqlContext.sql("""
    INSERT OVERWRITE TABLE index_SP500_list2

    SELECT
        *
    FROM
        tmp
    """)
    symbols = sqlContext.sql("select * from index_SP500_list2")
    for each in symbols.collect():
        print each
    sc.stop()


