#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SQLContext, HiveContext
from pyspark import SparkContext

import mat_close as mat

local_path = os.path.dirname(__file__)


def main(sc, sqlContext, isHive = True):
    dfMat = sqlContext.sql("""
        SELECT
            symbol, date, close, close_mat
        FROM
            %s
        WHERE
            symbol = "MSFT"
        LIMIT
            20
    """ % (mat.get_table_name(7)))
    lmat = dfMat.collect()
    print "summary: count:" , len(lmat)
    for each in dfMat.collect():
        print "%s\t%s\t%f\t%f" % (each.symbol, each.date, each.close, each.close_mat)



if __name__ == "__main__":
    sc = SparkContext(appName="bintrade_ta_summary")
    sqlContext = HiveContext(sc)
    sqlContext.sql("use fex")
    main(sc, sqlContext)
    sc.stop()
