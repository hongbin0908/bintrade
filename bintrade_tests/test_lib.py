#!/usr/bin/env python
# -*- coding:utf-8 -*-
#@author hongbin@youzan.com
import os, sys
local_path = os.path.dirname(__file__)
sys.path.append(local_path + "/../lib")

#import  lib.log as log

def test_debug():
    #log.debug("debug")
    a = eval("(1,[2,3])")
    print "xxxxxxx",a[1][0]
