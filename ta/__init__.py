#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")
sys.path.append(local_path + "/../")
sys.path.append(local_path)

from pyspark import SQLContext
from pyspark import SparkContext

import mat_close as mat
import summary as summary


def run(sc, sql_context, isHive):
    mat.main(sc, sql_context, isHive = True)
    summary.main(sc, sql_context, isHive = True)