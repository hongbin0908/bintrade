#!/usr/bin/env python
# -*- coding:utf-8 -*-
#@author hongbin@youzan.com
import os, sys
local_path = os.path.dirname(__file__)
print "local_path:", local_path
sys.path.append(local_path + "/./")

import logging
import time
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%Y%m%d %H:%M:%S',
                filename=os.path.join(local_path,"..",'log/bintrade.log.' + time.strftime('%Y-%m-%d')),
                filemode='a')


def debug(var):
   logging.debug(var)


if __name__ == '__main__':
    test()

