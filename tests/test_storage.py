#!/usr/bin/python
# coding: utf8

import unittest
import logging
import os


from pytsdb.models import Item
from pytsdb.storage import MemoryStorage, RedisStorage, CassandraStorage, SQLiteStorage


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

    def test_sqlitestore(self):
        storage = SQLiteStorage("test.db3")
        storage._createTable()
        self.assertTrue(storage)
        i = Item.new("test.ph", [(1000, 1.0)])
        storage.insert(i)
        i = Item.new("test.ph", [(2000, 4.0)])
        storage.insert(i)
        i = Item.new("test.ph", [(1100, 2.0)])
        storage.insert(i)
        i = Item.new("test.ph", [(1200, 3.0)])
        storage.insert(i)

        d = storage.get(key="test.ph", range_key=1000)
        self.assertEqual(d[0], (1000, 1.0))
        d = storage.get(key="test.ph", range_key=1100)
        self.assertEqual(d[0], (1100, 2.0))
        d = storage.get(key="test.ph", range_key=1200)
        self.assertEqual(d[0], (1200, 3.0))
        d = storage.get(key="test.ph", range_key=2000)
        self.assertEqual(d[0], (2000, 4.0))

        ds = storage.query(key="test.ph", range_min=1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-999, range_max=999)
        self.assertEqual(len(ds), 0)

        ds = storage.query(key="test.ph", range_min=1000, range_max=1200)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=1350)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=1101, range_max=1200)
        self.assertEqual(len(ds), 2)
        self.assertEqual(ds[0][0], (1100, 2.0))
        self.assertEqual(ds[1][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=999999)
        self.assertEqual(len(ds), 4)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))
        self.assertEqual(ds[3][0], (2000, 4.0))

        d = storage.last(key="test.ph")
        self.assertEqual(d[0], (2000, 4.0))

        d = storage.first(key="test.ph")
        self.assertEqual(d[0], (1000, 1.0))

        d = storage.left(key="test.ph", range_key=1050)
        self.assertEqual(d[0], (1000, 1.0))

    def test_cassandrastore(self):
        cassandra_host = os.getenv('CASSANDRA_HOST', 'localhost')
        cassandra_port = os.getenv('CASSANDRA_PORT', 9042)
        storage = CassandraStorage(contact_points=[cassandra_host], port=cassandra_port)
        storage._createTable()
        self.assertTrue(storage)
        i = Item.new("test.ph", [(1000, 1.0)])
        storage.insert(i)
        i = Item.new("test.ph", [(2000, 4.0)])
        storage.insert(i)
        i = Item.new("test.ph", [(1100, 2.0)])
        storage.insert(i)
        i = Item.new("test.ph", [(1200, 3.0)])
        storage.insert(i)

        d = storage.get(key="test.ph", range_key=1000)
        self.assertEqual(d[0], (1000, 1.0))
        d = storage.get(key="test.ph", range_key=1100)
        self.assertEqual(d[0], (1100, 2.0))
        d = storage.get(key="test.ph", range_key=1200)
        self.assertEqual(d[0], (1200, 3.0))
        d = storage.get(key="test.ph", range_key=2000)
        self.assertEqual(d[0], (2000, 4.0))

        ds = storage.query(key="test.ph", range_min=1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-999, range_max=999)
        self.assertEqual(len(ds), 0)

        ds = storage.query(key="test.ph", range_min=1000, range_max=1200)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=1350)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=1101, range_max=1200)
        self.assertEqual(len(ds), 2)
        self.assertEqual(ds[0][0], (1100, 2.0))
        self.assertEqual(ds[1][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=999999)
        self.assertEqual(len(ds), 4)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))
        self.assertEqual(ds[3][0], (2000, 4.0))

        d = storage.last(key="test.ph")
        self.assertEqual(d[0], (2000, 4.0))

        d = storage.first(key="test.ph")
        self.assertEqual(d[0], (1000, 1.0))

        d = storage.left(key="test.ph", range_key=1050)
        self.assertEqual(d[0], (1000, 1.0))

    def test_redisstore(self):
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = os.getenv('REDIS_PORT', 6379)
        storage = RedisStorage(host=redis_host, port=redis_port, db=0, expire=5)
        self.assertTrue(storage)
        i = Item.new("test.ph", [(1000, 1.0)])
        storage.insert(i)
        i = Item.new("test.ph", [(2000, 4.0)])
        storage.insert(i)
        i = Item.new("test.ph", [(1100, 2.0)])
        storage.insert(i)
        i = Item.new("test.ph", [(1200, 3.0)])
        storage.insert(i)

        d = storage.get(key="test.ph", range_key=1000)
        self.assertEqual(d[0], (1000, 1.0))
        d = storage.get(key="test.ph", range_key=1100)
        self.assertEqual(d[0], (1100, 2.0))
        d = storage.get(key="test.ph", range_key=1200)
        self.assertEqual(d[0], (1200, 3.0))
        d = storage.get(key="test.ph", range_key=2000)
        self.assertEqual(d[0], (2000, 4.0))

        ds = storage.query(key="test.ph", range_min=1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-999, range_max=999)
        self.assertEqual(len(ds), 0)

        ds = storage.query(key="test.ph", range_min=1000, range_max=1200)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=1350)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=1101, range_max=1200)
        self.assertEqual(len(ds), 2)
        self.assertEqual(ds[0][0], (1100, 2.0))
        self.assertEqual(ds[1][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=999999)
        self.assertEqual(len(ds), 4)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))
        self.assertEqual(ds[3][0], (2000, 4.0))

        d = storage.last(key="test.ph")
        self.assertEqual(d[0], (2000, 4.0))

        d = storage.first(key="test.ph")
        self.assertEqual(d[0], (1000, 1.0))

        d = storage.left(key="test.ph", range_key=1050)
        self.assertEqual(d[0], (1000, 1.0))

    def test_memorystore(self):
        storage = MemoryStorage()
        self.assertTrue(storage)
        i = Item.new("test.ph", [(1000, 1.0)])
        storage.insert(i)
        i = Item.new("test.ph", [(2000, 4.0)])
        storage.insert(i)
        i = Item.new("test.ph", [(1100, 2.0)])
        storage.insert(i)
        i = Item.new("test.ph", [(1200, 3.0)])
        storage.insert(i)

        d = storage.get(key="test.ph", range_key=1000)
        self.assertEqual(d[0], (1000, 1.0))
        d = storage.get(key="test.ph", range_key=1100)
        self.assertEqual(d[0], (1100, 2.0))
        d = storage.get(key="test.ph", range_key=1200)
        self.assertEqual(d[0], (1200, 3.0))
        d = storage.get(key="test.ph", range_key=2000)
        self.assertEqual(d[0], (2000, 4.0))

        ds = storage.query(key="test.ph", range_min=1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-999, range_max=999)
        self.assertEqual(len(ds), 0)

        ds = storage.query(key="test.ph", range_min=1000, range_max=1200)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=1350)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=1101, range_max=1200)
        self.assertEqual(len(ds), 2)
        self.assertEqual(ds[0][0], (1100, 2.0))
        self.assertEqual(ds[1][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=999999)
        self.assertEqual(len(ds), 4)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))
        self.assertEqual(ds[3][0], (2000, 4.0))

        d = storage.last(key="test.ph")
        self.assertEqual(d[0], (2000, 4.0))

        d = storage.first(key="test.ph")
        self.assertEqual(d[0], (1000, 1.0))

        d = storage.left(key="test.ph", range_key=1050)
        self.assertEqual(d[0], (1000, 1.0))
