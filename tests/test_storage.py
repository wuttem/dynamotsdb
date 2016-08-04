#!/usr/bin/python
# coding: utf8

import unittest
import logging
import random


from pytsdb.storage import MemoryStorage
from pytsdb.models import Item


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

    def test_split(self):
        d = []
        for i in range(100):
            d.append((i, i * 2))
        i = Item("ph")
        i.insert(d)
        self.assertEqual(len(i), 100)
        buckets = i.split_item(30)
        self.assertEqual(len(buckets), 4)
        self.assertEqual(len(buckets[0]), 30)
        self.assertEqual(len(buckets[1]), 30)
        self.assertEqual(len(buckets[2]), 30)
        self.assertEqual(len(buckets[3]), 10)
        self.assertEqual(len(i), 30)

    def test_rawitem(self):
        d = []
        for i in range(100):
            d.append((i, i * 2))
        self.assertEqual(len(d), 100)

        d1 = list(d[:50])
        d2 = list(d[50:])
        random.shuffle(d1)
        random.shuffle(d2)

        i = Item("ph")
        for t, v in d1:
            i.insert_point(t, v)
        i.insert(d2)

        l = i.to_list()
        self.assertEqual(len(l), 100)
        logging.warning(l)
        for i in range(100):
            self.assertEqual(l[i][0], i)
            self.assertEqual(l[i][1], i * 2)

    def test_binaryrepr(self):
        d = []
        for i in range(4):
            d.append((i, i * 2))
        self.assertEqual(len(d), 4)
        random.shuffle(d)
        i = Item("ph", d)
        s = i.to_string()
        self.assertEqual(len(s), 4 * 2 * 4 + 4)
        i2 = Item.from_string("ph", s)
        self.assertEqual(i._values, i2._values)
        self.assertEqual(i._timestamps, i2._timestamps)

        d = [(2**16 - 1, 6.0)]
        i = Item("ph", d)
        s = i.to_string()
        self.assertEqual(s.encode("hex"), '01000000ffff00000000c040')

    def test_store(self):
        l = MemoryStorage()
        l._insert(key="test.ph", range_key=1000, data={"foo": "bar1"})
        logging.warning(l.cache)
        l._insert(key="test.ph", range_key=2000, data={"foo": "bar4"})
        logging.warning(l.cache)
        l._insert(key="test.ph", range_key=1100, data={"foo": "bar2"})
        logging.warning(l.cache)
        l._insert(key="test.ph", range_key=1200, data={"foo": "bar3"})
        logging.warning(l.cache)

        d = l._get(key="test.ph", range_key=1000)
        self.assertEqual(d.data, {"foo": "bar1"})
        d = l._get(key="test.ph", range_key=1100)
        self.assertEqual(d.data, {"foo": "bar2"})
        d = l._get(key="test.ph", range_key=1200)
        self.assertEqual(d.data, {"foo": "bar3"})
        d = l._get(key="test.ph", range_key=2000)
        self.assertEqual(d.data, {"foo": "bar4"})

        ds = l._query(key="test.ph", range_min=1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0].data, {"foo": "bar1"})

        ds = l._query(key="test.ph", range_min=-1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0].data, {"foo": "bar1"})

        ds = l._query(key="test.ph", range_min=-999, range_max=999)
        self.assertEqual(len(ds), 0)

        ds = l._query(key="test.ph", range_min=1000, range_max=1200)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0].data, {"foo": "bar1"})
        self.assertEqual(ds[1].data, {"foo": "bar2"})
        self.assertEqual(ds[2].data, {"foo": "bar3"})

        ds = l._query(key="test.ph", range_min=99, range_max=1350)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0].data, {"foo": "bar1"})
        self.assertEqual(ds[1].data, {"foo": "bar2"})
        self.assertEqual(ds[2].data, {"foo": "bar3"})

        ds = l._query(key="test.ph", range_min=1101, range_max=1200)
        self.assertEqual(len(ds), 2)
        self.assertEqual(ds[0].data, {"foo": "bar2"})
        self.assertEqual(ds[1].data, {"foo": "bar3"})

        ds = l._query(key="test.ph", range_min=99, range_max=999999)
        self.assertEqual(len(ds), 4)
        self.assertEqual(ds[0].data, {"foo": "bar1"})
        self.assertEqual(ds[1].data, {"foo": "bar2"})
        self.assertEqual(ds[2].data, {"foo": "bar3"})
        self.assertEqual(ds[3].data, {"foo": "bar4"})

        d = l._last(key="test.ph")
        self.assertEqual(d.data, {"foo": "bar4"})

        d = l._first(key="test.ph")
        self.assertEqual(d.data, {"foo": "bar1"})
