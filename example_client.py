#!/usr/bin/python
# coding: utf8

import time
import random
from pytsdb import TSDB

def onData(key, info):
    print("New Data: {} - {}".format(key, info))

db = TSDB(storage="cassandra")
db._register_data_listener("temp", onData)

try:
    while True:
        time.sleep(0.1)
except KeyboardInterrupt:
    db._close()
    print 'bye bye'
