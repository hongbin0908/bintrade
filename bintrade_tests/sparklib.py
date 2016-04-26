import os
import sys

from pyspark import SQLContext
from pyspark import SparkContext

os.environ["SPARK_HOME"] = "/Users/abin/spark-1.6.1-bin-hadoop2.6"
sc = SparkContext('local[1]')
sqlContext = SQLContext(sc)
sqlContext.setConf( "spark.sql.shuffle.partitions", "1")