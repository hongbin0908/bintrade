#!/usr/bin/env python
# -*- coding:utf-8 -*-
#@author hongbin@youzan.com
import os, sys
local_path = os.path.dirname(__file__)

import time, datetime

def get_week_day(str_date):
    return datetime.datetime.strptime(str_date,"%Y-%m-%d").weekday()

def test_get_week_day():
    assert get_week_day("2016-04-21") == 3