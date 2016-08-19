#!/usr/bin/python
# coding: utf8

import time
import random
import itertools
from multiprocessing import Pool
from pytsdb import TSDB

PATCH_SIZES = [3, 5, 10, 30, 50]
DATA_POINTS = 10000
TIME_OFFSET = int(time.time() - 365*24*60*60)
WORKERS = 1


db = TSDB(storage="sqlite")
db.storage._createTable()


def insertTest(points, patch, sensor="sensor0"):
    i = 0
    while i < points:
        d = [(TIME_OFFSET+(i+x)*600, float(random.randint(20,25)))
             for x in range(patch)]
        key = "{}.temp.{}".format(sensor, patch)
        db._insert(key, d)
        i += patch
    return i


def insertTest_star(args):
    return insertTest(*args)


if __name__ == '__main__':
    pool = Pool(processes=WORKERS)
    points = DATA_POINTS / WORKERS
    for patch in PATCH_SIZES:
        t = time.time()
        args_1 = [points for x in range(WORKERS)]
        args_2 = [patch for x in range(WORKERS)]
        args_3 = ["sensor{}".format(x) for x in range(WORKERS)]
        args = itertools.izip(args_1, args_2, args_3)
        res = pool.map(insertTest_star, args)
        print(res)
        t = time.time() - t
        p_s = DATA_POINTS / t
        print("Patch Size: {} - {}s - {} points/sec".format(patch, t, p_s))
