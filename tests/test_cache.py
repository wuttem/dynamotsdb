#!/usr/bin/python
# coding: utf8

import unittest
import logging
import os
import time


from pytsdb.cache import RedisLRU


class CacheTest(unittest.TestCase):
    def setUp(self):
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = os.getenv('REDIS_PORT', 6379)
        self.c = RedisLRU(host=redis_host, port=redis_port, db=0)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)

    def test_basic(self):
        self.c.clear()
        self.c.setup_namespace("test", 3)
        self.c.clear("test")
        self.c.store("a", 1, namespace="test")
        time.sleep(0.01)
        self.c.store("b", 2, namespace="test")
        time.sleep(0.01)
        self.c.store("c", 3, namespace="test")
        time.sleep(0.01)

        res = self.c.get("a", namespace="test")
        self.assertEqual(res, 1)
        time.sleep(0.01)

        self.c.store("d", 4, namespace="test")
        time.sleep(0.01)
        self.c.store("e", 5, namespace="test")
        time.sleep(0.01)

        res = self.c.get("a", namespace="test")
        self.assertEqual(res, 1)
        res = self.c.get("b", namespace="test")
        self.assertEqual(res, None)
        res = self.c.get("c", namespace="test")
        self.assertEqual(res, None)
        res = self.c.get("d", namespace="test")
        self.assertEqual(res, 4)
        res = self.c.get("e", namespace="test")
        self.assertEqual(res, 5)

        self.c.expire("a", namespace="test")
        res = self.c.get("a", namespace="test")
        self.assertEqual(res, None)

        res = self.c.get("a")
        self.assertEqual(res, None)

        with self.assertRaises(KeyError):
            res = self.c.get("a", namespace="sadsfdf")
