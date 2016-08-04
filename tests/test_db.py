#!/usr/bin/python
# coding: utf8

import unittest
import random
import logging


from pytsdb import TSDB


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

    def test_basic(self):
        d = TSDB(BUCKETSIZE_TARGET=3, BUCKETSIZE_MAX=3)
        d._insert("hi", [(1, 1.1), (2, 2.2)])
        d._insert("hi", [(4, 4.4)])
        i = d.storage.last("hi")
        self.assertEqual(len(i), 3)
        self.assertEqual(i[0][0], 1)
        self.assertEqual(i[1][0], 2)
        self.assertEqual(i[2][0], 4)

        d._insert("hi", [(3, 3.3)])
        buckets = d.storage.query("hi", 0, 10)
        self.assertEqual(len(buckets), 2)
        i = buckets[0]
        self.assertEqual(len(i), 3)
        self.assertEqual(i[0][0], 1)
        self.assertEqual(i[1][0], 2)
        self.assertEqual(i[2][0], 3)

        i2 = buckets[1]
        self.assertEqual(len(i2), 1)
        self.assertEqual(i2[0][0], 4)

    def test_largedataset(self):
        # Generate
        d = []
        for i in range(50000):
            d.append((i, i * 2.5))

        s = []
        while len(d) > 0:
            count = random.randint(3, 30)
            s.append([])
            for _ in range(count):
                if len(d) < 1:
                    break
                el = d.pop(0)
                s[-1].append(el)

        # Make some holes
        s.insert(200, s.pop(100))
        s.insert(200, s.pop(100))
        s.insert(200, s.pop(100))
        s.insert(1000, s.pop(1100))
        s.insert(1200, s.pop(1300))
        s.insert(1400, s.pop(1400))

        # Strange Future Hole
        s.insert(2000, s.pop(1800))

        # Insert
        d = TSDB(BUCKETSIZE_TARGET=100)
        for p in s:
            d._insert("ph", p)

        buckets = d.storage.query("ph", 0, 50000)
        self.assertGreater(len(buckets), 450)
        self.assertLess(len(buckets), 550)
