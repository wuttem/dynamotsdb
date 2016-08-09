#!/usr/bin/python
# coding: utf8

import unittest
import logging
import os


from pytsdb.storage import MemoryStorage, RedisStorage


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

    def test_redisstore(self):
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = os.getenv('REDIS_PORT', 6379)
        l = RedisStorage(host=redis_host, port=redis_port, db=0, expire=5)
        self.assertTrue(l)
        l._insert(key="test.ph", range_key=1000, data="bar1")
        l._insert(key="test.ph", range_key=2000, data="bar4")
        l._insert(key="test.ph", range_key=1100, data="bar2")
        l._insert(key="test.ph", range_key=1200, data="bar3")

        d = l._get(key="test.ph", range_key=1000)
        self.assertEqual(d, "bar1")
        d = l._get(key="test.ph", range_key=1100)
        self.assertEqual(d, "bar2")
        d = l._get(key="test.ph", range_key=1200)
        self.assertEqual(d, "bar3")
        d = l._get(key="test.ph", range_key=2000)
        self.assertEqual(d, "bar4")

        ds = l._query(key="test.ph", range_min=1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0], "bar1")

        ds = l._query(key="test.ph", range_min=-1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0], "bar1")

        ds = l._query(key="test.ph", range_min=-999, range_max=999)
        self.assertEqual(len(ds), 0)

        ds = l._query(key="test.ph", range_min=1000, range_max=1200)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0], "bar1")
        self.assertEqual(ds[1], "bar2")
        self.assertEqual(ds[2], "bar3")

        ds = l._query(key="test.ph", range_min=99, range_max=1350)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0], "bar1")
        self.assertEqual(ds[1], "bar2")
        self.assertEqual(ds[2], "bar3")

        ds = l._query(key="test.ph", range_min=1101, range_max=1200)
        self.assertEqual(len(ds), 2)
        self.assertEqual(ds[0], "bar2")
        self.assertEqual(ds[1], "bar3")

        ds = l._query(key="test.ph", range_min=99, range_max=999999)
        self.assertEqual(len(ds), 4)
        self.assertEqual(ds[0], "bar1")
        self.assertEqual(ds[1], "bar2")
        self.assertEqual(ds[2], "bar3")
        self.assertEqual(ds[3], "bar4")

        d = l._last(key="test.ph")
        self.assertEqual(d, "bar4")

        d = l._first(key="test.ph")
        self.assertEqual(d, "bar1")

    def test_memorystore(self):
        l = MemoryStorage()
        self.assertTrue(l)
        l._insert(key="test.ph", range_key=1000, data="bar1")
        l._insert(key="test.ph", range_key=2000, data="bar4")
        l._insert(key="test.ph", range_key=1100, data="bar2")
        l._insert(key="test.ph", range_key=1200, data="bar3")

        d = l._get(key="test.ph", range_key=1000)
        self.assertEqual(d, "bar1")
        d = l._get(key="test.ph", range_key=1100)
        self.assertEqual(d, "bar2")
        d = l._get(key="test.ph", range_key=1200)
        self.assertEqual(d, "bar3")
        d = l._get(key="test.ph", range_key=2000)
        self.assertEqual(d, "bar4")

        ds = l._query(key="test.ph", range_min=1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0], "bar1")

        ds = l._query(key="test.ph", range_min=-1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0], "bar1")

        ds = l._query(key="test.ph", range_min=-999, range_max=999)
        self.assertEqual(len(ds), 0)

        ds = l._query(key="test.ph", range_min=1000, range_max=1200)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0], "bar1")
        self.assertEqual(ds[1], "bar2")
        self.assertEqual(ds[2], "bar3")

        ds = l._query(key="test.ph", range_min=99, range_max=1350)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0], "bar1")
        self.assertEqual(ds[1], "bar2")
        self.assertEqual(ds[2], "bar3")

        ds = l._query(key="test.ph", range_min=1101, range_max=1200)
        self.assertEqual(len(ds), 2)
        self.assertEqual(ds[0], "bar2")
        self.assertEqual(ds[1], "bar3")

        ds = l._query(key="test.ph", range_min=99, range_max=999999)
        self.assertEqual(len(ds), 4)
        self.assertEqual(ds[0], "bar1")
        self.assertEqual(ds[1], "bar2")
        self.assertEqual(ds[2], "bar3")
        self.assertEqual(ds[3], "bar4")

        d = l._last(key="test.ph")
        self.assertEqual(d, "bar4")

        d = l._first(key="test.ph")
        self.assertEqual(d, "bar1")
