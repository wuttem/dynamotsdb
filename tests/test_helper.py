#!/usr/bin/python
# coding: utf8

import unittest
import random
import logging
import datetime
import time


from pytsdb.helper import to_ts, from_ts


class StorageTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)
        pass

    def test_findsplits(self):
        # Left hourly
        from pytsdb.helper import ts_hourly_left
        t = time.time()
        t = ts_hourly_left(t)
        h = datetime.datetime.utcnow().replace(minute=0, second=0,
                                               microsecond=0)
        h = to_ts(h)

        # Left daily
        from pytsdb.helper import ts_daily_left
        t = time.time()
        t = ts_daily_left(t)
        h = datetime.datetime.utcnow().replace(hour=0,
                                               minute=0, second=0,
                                               microsecond=0)
        h = to_ts(h)

        # Left weekly
        from pytsdb.helper import ts_weekly_left
        t = 1470388856.652508
        t = ts_weekly_left(t)
        h = datetime.datetime(2016, 8, 1, 0, 0, 0)
        h = to_ts(h)

        # Left monthly
        from pytsdb.helper import ts_monthly_left
        t = time.time()
        t = ts_monthly_left(t)
        h = datetime.datetime.utcnow().replace(day=1,
                                               hour=0,
                                               minute=0, second=0,
                                               microsecond=0)
        h = to_ts(h)
