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
        def test_function1(key, event):
            messages.append((key, event))
        self.r.register_callback("device.*", test_function1)

        self.assertEqual(len(messages), 0)
        self.r.publish_event("xyz", ts_min=1, ts_max=2, count=2)
        time.sleep(0.1)
        self.assertEqual(len(messages), 0)

        self.r.publish_event("device.xyz", ts_min=1, ts_max=2, count=2)
        time.sleep(0.1)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[-1][0], "device.xyz")
        self.assertEqual(messages[-1][1].ts_min, 1)
        self.assertEqual(messages[-1][1].ts_max, 2)
        self.assertEqual(messages[-1][1].count, 2)

        self.r.publish_event("device.abc", ts_min=2, ts_max=2, count=1)
        time.sleep(0.1)
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[-1][0], "device.abc")
        self.assertEqual(messages[-1][1].ts_min, 2)
        self.assertEqual(messages[-1][1].ts_max, 2)
        self.assertEqual(messages[-1][1].count, 1)

        time.sleep(0.5)
        self.r.close()

    def test_raisingcallback(self):
        def test_function2(key, event):
            raise ValueError("Mein Fehler ...")
        self.r.register_callback("device.*", test_function2)
        self.assertEqual(self.r._last_error, None)
        self.r.publish_event("device.xyz", ts_min=1, ts_max=2, count=2)
        time.sleep(0.1)
        self.assertIn("Mein Fehler", self.r._last_error)
