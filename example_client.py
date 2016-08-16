#!/usr/bin/python
# coding: utf8

import time
import random
from pytsdb import TSDB


db = TSDB(storage="cassandra")

def onData(key, event):
    print("New: {}".format(event))
    data = db._query(event.key, event.ts_min, event.ts_max)
    print(data.pretty_print())
    print("---")

db._register_data_listener("temp", onData)

try:
    while True:
        time.sleep(0.1)
except KeyboardInterrupt:
    db._close()
    print 'bye bye'
