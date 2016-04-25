#!/usr/bin/env python
# -*- coding:utf-8 -*-
#@author hongbin@youzan.com
import os, sys
local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")


import bintrade_tests.sparklib as slib

def test_randomforest():
    data = slib.sqlContext.read.format("libsvm").load(local_path + "/sample_libsvm_data.txt")
    print data.printSchema()