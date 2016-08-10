#!/usr/bin/python
# coding: utf8

import unittest
import logging
import os
import time


from pytsdb.events import RedisPubSub


class EventTest(unittest.TestCase):
    def setUp(self):
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = os.getenv('REDIS_PORT', 6379)
        self.r = RedisPubSub(host=redis_host, port=redis_port, db=0)

    def tearDown(self):
        self.r.close()

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)

    def test_callback(self):
        messages = []
        def test_function(key, info):
            messages.append((key, info))
        self.r.register_callback("device.*", test_function)

        self.assertEqual(len(messages), 0)
        self.r.new_data("xyz", 1, 2)
        time.sleep(0.1)
        self.assertEqual(len(messages), 0)

        self.r.new_data("device.xyz", 1, 2)
        time.sleep(0.1)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0][0], "device.xyz")
        self.assertEqual(messages[0][1]["range_min"], 1)
        self.assertEqual(messages[0][1]["range_max"], 2)

        self.r.new_data("device.abc", 2, 2)
        time.sleep(0.1)
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[1][0], "device.abc")
        self.assertEqual(messages[1][1]["range_min"], 2)
        self.assertEqual(messages[1][1]["range_max"], 2)
