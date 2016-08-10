#!/usr/bin/python
# coding: utf8

import unittest
import random
import logging
import binascii
import datetime

from pytsdb.models import Item, ItemType, Aggregation, TupleArray
from pytsdb.models import ResultSet
from pytsdb.helper import to_ts


class ModelTest(unittest.TestCase):
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

    def test_aggregations(self):
        ts = to_ts(datetime.datetime(2000, 1, 1, 0, 0))
        items = []
        for i in range(10):
            items.append(Item("d"))
            for j in range(144):
                items[-1].insert_point(ts + j * 600, float(j % 6))
            ts += 144 * 600

        res = ResultSet("d", items)
        # All
        self.assertEqual(len(list(res.all())), 144 * 10)

        # Daily
        daily = list(res.daily())
        self.assertEqual(len(daily), 10)
        self.assertEqual(len(list(daily[0])), 144)

        # Daily Aggr
        g = res.aggregation("daily", "sum")
        for x in g:
            self.assertEqual(x[1], 360.0)

        g = res.aggregation("daily", "count")
        for x in g:
            self.assertEqual(x[1], 144)

        g = res.aggregation("daily", "mean")
        for x in g:
            self.assertEqual(x[1], 2.5)

        g = res.aggregation("daily", "min")
        for x in g:
            self.assertEqual(x[1], 0.0)

        g = res.aggregation("daily", "max")
        for x in g:
            self.assertEqual(x[1], 5.0)

        g = res.aggregation("daily", "amp")
        for x in g:
            self.assertEqual(x[1], 5.0)

        # Hourly
        daily = list(res.daily())
        self.assertEqual(len(daily), 10)
        self.assertEqual(len(list(daily[0])), 144)

        # Hourly Aggr
        g = res.aggregation("hourly", "sum")
        for x in g:
            self.assertEqual(x[1], 15.0)

        g = res.aggregation("hourly", "count")
        for x in g:
            self.assertEqual(x[1], 6)

        g = res.aggregation("hourly", "mean")
        for x in g:
            self.assertEqual(x[1], 2.5)

        g = res.aggregation("hourly", "min")
        for x in g:
            self.assertEqual(x[1], 0.0)

        g = res.aggregation("hourly", "max")
        for x in g:
            self.assertEqual(x[1], 5.0)

        g = res.aggregation("hourly", "amp")
        for x in g:
            self.assertEqual(x[1], 5.0)

    def test_item(self):
        i1 = Item("test", item_type=ItemType.tuple_float_3)
        self.assertFalse(i1)
        self.assertFalse(i1._dirty)
        self.assertFalse(i1.existing)
        i1.insert_point(1, (1.0, 2.0, 3.0))
        self.assertTrue(i1)
        self.assertTrue(i1._dirty)

        i2 = Item("test1", [(1, 1.0)])
        self.assertEqual(i2[0], (1, 1.0))
        i2.insert_point(1, 2.0)
        self.assertEqual(i2[0], (1, 1.0))
        i2.insert_point(1, 2.0, overwrite=True)
        self.assertEqual(i2[0], (1, 2.0))

        i3 = Item("test2", [(1, 1.0)])
        self.assertNotEqual(i2, i3)

        i4 = Item("test1", [(1, 1.0)])
        self.assertEqual(i2, i4)

    def test_tuplearray(self):
        t = TupleArray("f", 2)
        self.assertEqual(len(t), 0)
        t.append((4.5, 6.5))
        t.insert(0, (2.5, 4.5))
        self.assertEqual(len(t), 2)
        self.assertEqual(t[0], (2.5, 4.5))
        self.assertEqual(t[1], (4.5, 6.5))
        del t[0]
        self.assertEqual(len(t), 1)
        self.assertEqual(t[0], (4.5, 6.5))
        t[0] = (2.5, 4.5)
        self.assertEqual(t[0], (2.5, 4.5))
        with self.assertRaises(ValueError):
            t[0] = (2.5, 2.5, 2.5)
        with self.assertRaises(ValueError):
            t.append((2.5, 2.5, 2.5))
        with self.assertRaises(ValueError):
            t.insert(0, (2.5, 2.5, 2.5))
        with self.assertRaises(TypeError):
            t.append(3)
        self.assertTrue(str(t))

    def test_split(self):
        d = []
        for i in range(100):
            d.append((i, i * 2))
        i = Item("ph")
        i.insert(d)
        self.assertEqual(len(i), 100)
        buckets = i._split_item(30)
        self.assertEqual(len(buckets), 4)
        self.assertEqual(len(buckets[0]), 30)
        self.assertEqual(len(buckets[1]), 30)
        self.assertEqual(len(buckets[2]), 30)
        self.assertEqual(len(buckets[3]), 10)
        self.assertEqual(len(i), 30)

    def test_intdata(self):
        i = Item("int", item_type=ItemType.raw_int)
        for j in range(10):
            i.insert_point(j, int(j * 2.1))
        self.assertEqual(len(i), 10)
        self.assertEqual(i[3], (3, 6))
        s = i.to_string()
        i = Item.from_string("int", s)
        self.assertEqual(len(i), 10)
        self.assertEqual(i[3], (3, 6))

    def test_aggregateddata(self):
        i = Item("t", item_type=ItemType.basic_aggregation)
        for j in range(11):
            i.insert_point(j, Aggregation(min=j, max=j * 2,
                                          count=2, sum=j * 3))
        self.assertEqual(len(i), 11)
        self.assertEqual(i[3], (3, Aggregation(min=3, max=3 * 2,
                                               count=2, sum=3 * 3)))
        s = i.to_string()
        i = Item.from_string("t", s)
        self.assertEqual(len(i), 11)
        self.assertEqual(i[3], (3, Aggregation(min=3, max=3 * 2,
                                               count=2, sum=3 * 3)))
        self.assertEqual(i[5][1].min, 5.0)
        self.assertEqual(i[5][1].max, 10.0)
        self.assertEqual(i[5][1].count, 2)
        self.assertEqual(i[5][1].sum, 15.0)

    def test_tupledata(self):
        i = Item("t", item_type=ItemType.tuple_float_2)
        for j in range(11):
            i.insert_point(j, (j * 2.5, j * 3.0))
        self.assertEqual(len(i), 11)
        self.assertEqual(i[3], (3, (7.5, 9.0)))
        s = i.to_string()
        i = Item.from_string("t", s)
        self.assertEqual(len(i), 11)
        self.assertEqual(i[3], (3, (7.5, 9.0)))

    def test_rawitem(self):
        d = []
        for i in range(100):
            d.append((i, i * 2.5))
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
            self.assertEqual(l[i][1], i * 2.5)

    def test_binaryrepr(self):
        d = []
        for i in range(4):
            d.append((i, i * 2))
        self.assertEqual(len(d), 4)
        random.shuffle(d)
        i = Item("ph", d)
        s = i.to_string()
        self.assertEqual(len(s), 4 * 2 * 4 + Item.HEADER_SIZE)
        i2 = Item.from_string("ph", s)
        self.assertEqual(i._values, i2._values)
        self.assertEqual(i._timestamps, i2._timestamps)

        d = [(2**16 - 1, 6.0)]
        i = Item("ph", d)
        s = i.to_string()
        self.assertEqual(binascii.hexlify(s),
                         b'0100010001000000ffff00000000c040')
