#!/usr/bin/python
# coding: utf8

import time
import random
from pytsdb import TSDB

db = TSDB(storage="cassandra")

try:
    while True:
        t = time.time()
        v = float(random.randint(20,25))
        db._insert("temp", [(time.time(), v)])
        print("{} temp: {}".format(t, v))
        time.sleep(1.0)
except KeyboardInterrupt:
    print 'bye bye'
