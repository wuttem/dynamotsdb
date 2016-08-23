#!/usr/bin/python
# coding: utf8

import time
import random
from pytsdb import TSDB

db = TSDB(STORAGE="cassandra")

try:
    while True:
        t = time.time()
        v = float(random.randint(20,25))
        db._insert("temp", [(time.time(), v)])
        print("{} temp: {}".format(t, v))
        time.sleep(0.7)
except KeyboardInterrupt:
    print 'bye bye'
