#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author hongbin@youzan.com
import os
import sys

from pyspark import SQLContext
from pyspark import SparkContext

import bintrade_tests.sparklib as slib
import eod.msft as msft

local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")

def  test_create_table():
    pass

def test_retrivalNewestMsDate():
    ldEod = msft.retrivalNewestMsDate()
    print len(ldEod)
    assert 7568 <= len(ldEod)