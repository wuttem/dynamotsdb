#!/usr/bin/python
# coding: utf8

import time
import random
import itertools
from pytsdb import TSDB

PATCH_SIZES = [3, 5, 10, 30, 50]
DATA_POINTS = 10000
TIME_OFFSET = int(time.time() - 365*24*60*60)


db = TSDB(storage="cassandra", BUCKET_TYPE="daily")


def insertTest(points, patch, sensor="sensor0"):
    i = 0
    while i < points:
        d = [(TIME_OFFSET+(i+x)*600, float(random.randint(20,25)))
             for x in range(patch)]
        key = "{}.phe.{}".format(sensor, patch)
        db._insert(key, d)
        i += patch
    return i


if __name__ == '__main__':
    db.storage._createTable()

    for patch in PATCH_SIZES:
        print("Testing Patch: {}".format(patch))
        t = time.time()
        res = insertTest(DATA_POINTS, patch, "sensor0")
        print(res)
        t = time.time() - t
        p_s = DATA_POINTS / t
        print("Patch Size: {} - {}s - {} points/sec".format(patch, t, p_s))
