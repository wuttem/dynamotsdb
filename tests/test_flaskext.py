#!/usr/bin/python
# coding: utf8

import unittest
import os
import random
import logging
import datetime
import time
from flask import Flask

from pytsdb import FlaskTSDB
from flask import Flask


class HelperTest(unittest.TestCase):
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

    def test_flask(self):
        cassandra_host = os.getenv('CASSANDRA_HOST', 'localhost')
        cassandra_port = os.getenv('CASSANDRA_PORT', 9042)
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = os.getenv('REDIS_PORT', 6379)
        redis_db = 1

        app = Flask("testapp")
        app.config.update(
            CASSANDRA_HOST=cassandra_host,
            CASSANDRA_PORT=cassandra_port,
            REDIS_HOST=redis_host,
            REDIS_PORT=redis_port,
            REDIS_DB=redis_db,
            ENABLE_EVENTS=True,
            ENABLE_CACHING=True
        )
        ext = FlaskTSDB()
        ext.init_app(app)

        with self.assertRaises(RuntimeError):
            self.assertTrue(ext.db)

        # Push Context
        ctx = app.app_context()
        ctx.push()
        self.assertTrue(ext.db)
        ctx.pop()
        # Test again
        ctx.push()
        self.assertTrue(ext.db)

        # Test Insert
        i = ext.insert("testsensor.ph", [(1000, 2.5), (1001, 2.5)])
        self.assertEqual(i["count"], 2)

        # Test Stats
        s = ext.stats("testsensor.ph")
        self.assertEqual(s["count"], 2)

        # Test Query
        q = ext.query("testsensor.ph", 1000, 1001)
        self.assertEqual(len(list(q.all())), 2)

        # Tear Down
        ctx.pop()