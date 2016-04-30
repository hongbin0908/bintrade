#!/usr/bin/env python
# -*- coding:utf-8 -*-
#@author hongbin@youzan.com
import os, sys
local_path = os.path.dirname(__file__)

import time, datetime
from collections import namedtuple

def get_week_day(str_date):
    return datetime.datetime.strptime(str_date,"%Y-%m-%d").weekday()

def test_get_week_day():
    assert get_week_day("2016-04-21") == 3

def dictify(some_named_tuple): 
    return dict((s, getattr(some_named_tuple, s)) for s in some_named_tuple._fields) 

if __name__ == "__main__":
    Point = namedtuple('Point', ['x', 'y'])
    p = Point(11, y=22)  
    print dictify(p)
