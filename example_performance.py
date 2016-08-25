#!/usr/bin/python
# coding: utf8

import time
import random
import logging
from pytsdb import TSDB

PATCH_SIZES = [3, 10, 30, 50]
DATA_POINTS = 1000
TIME_OFFSET = int(time.time() - 365*24*60*60)
TIME_OFFSET = TIME_OFFSET - (TIME_OFFSET % (24 * 60 * 60))


db = TSDB(STORAGE="cassandra", BUCKET_TYPE="daily", ENABLE_CACHING=True)


def insertTest(points, patch, sensor="sensor0"):
    i = 0
    while i < points:
        d = [(TIME_OFFSET+(i+x)*600, float(random.randint(20,25)))
             for x in range(patch)]
        key = "{}.phex.{}".format(sensor, patch)
        db._insert(key, d)
        i += patch
    return i


if __name__ == '__main__':
    db.storage._createTable()
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger(__name__)
    logger.info("Starting Test Script With TS: {}".format(TIME_OFFSET))

    for patch in PATCH_SIZES:
        print("Testing Patch: {}".format(patch))
        t = time.time()
        res = insertTest(DATA_POINTS, patch, "test06")
        print(res)
        t = time.time() - t
        p_s = DATA_POINTS / t
        print("Patch Size: {} - {}s - {} points/sec".format(patch, t, p_s))
