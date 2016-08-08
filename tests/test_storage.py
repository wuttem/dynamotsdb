#!/usr/bin/python
# coding: utf8

import unittest
import logging


from pytsdb.storage import MemoryStorage


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
