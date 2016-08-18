#!/usr/bin/python
# coding: utf8

import time
import random
from pytsdb import TSDB

PATCH_SIZES = [3]
DATA_POINTS = 10000
TIME_OFFSET = int(time.time() - 365*24*60*60)


db = TSDB(storage="cassandra")


def insertTest(points, patch):
    i = 0
    t = time.time()
    while i < points:
        d = [(TIME_OFFSET+(i+x)*600, float(random.randint(20,25)))
             for x in range(patch)]
        key = "testm_{}".format(patch)
        db._insert(key, d)
        i += patch
    return time.time() - t


for p in PATCH_SIZES:
    res = DATA_POINTS / insertTest(DATA_POINTS, p)
    print("Patch Size: {} - {} points/sec".format(p, res))
