#!/usr/bin/python
# coding: utf8

import unittest


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
        pass

    def test_basic(self):
        d = TSDB(BUCKETSIZE_TARGET=3)
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
