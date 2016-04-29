import os
import sys

from pyspark import SQLContext
from pyspark import SparkContext

#os.environ["SPARK_HOME"] = "/opt/spark-1.6.1-bin-hadoop2.6"
#os.environ["HADOOP_HOME"] = "/opt/hadoop"
#os.environ["HADOOP_PREFIX"] = "/opt/hadoop"

#os.environ["HIVE_HOME"] = "/opt/hive"


sc = SparkContext('local[1]')
sql_context = SQLContext(sc)
sql_context.setConf( "spark.sql.shuffle.partitions", "1")
sql_context.sql(""" use fex_test """)
