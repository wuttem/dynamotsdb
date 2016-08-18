#!/usr/bin/python
# coding: utf8

from __future__ import unicode_literals
import bisect
from redis import StrictRedis as Redis
from cassandra.cluster import Cluster
from collections import namedtuple
from .errors import NotFoundError, ConflictError
from .models import Item


Element = namedtuple('Element', ['key', 'range_key', 'data'])


class Storage(object):
    def _to_item(self, key, data):
        raise NotImplementedError("child class must implement _to_item")

    def _from_item(self, item):
        raise NotImplementedError("child class must implement _from_item")

    def get(self, key, range_key):
        return self._to_item(key, self._get(key, range_key))

    def insert(self, item):
        self._insert(**self._from_item(item))

    def update(self, item):
        self._update(**self._from_item(item))

    def query(self, key, range_min, range_max):
        out = list()
        for i in self._query(key, range_min, range_max):
            out.append(self._to_item(key, i))
        return out

    def last(self, key):
        return self._to_item(key, self._last(key))

    def first(self, key):
        return self._to_item(key, self._first(key))

    def left(self, key, range_key):
        return self._to_item(key, self._left(key, range_key))

    def stats(self, key):
        return {"ts_min": self.ts_min(key), "ts_max": self.ts_max(key),
                "count": self.count(key)}

    def ts_min(self, key):
        i = self.first(key)
        return i.ts_min

    def ts_max(self, key):
        i = self.last(key)
        return i.ts_max

    def count(self, key):
        items = self.query(key, 0, (2**31)-1)
        count = 0
        for i in items:
            count += i.count
        return count

class CassandraStorage(Storage):
    def __init__(self, **kwargs):
        self._cassandra = Cluster(**kwargs)
        self._session = None
        self.key_space = "test"
        self.table_name = "{}.testtable".format(self.key_space)

    @property
    def cassandra(self):
        if self._session is None:
            self._session = self._cassandra.connect()
        return self._session

    def _createTable(self):
        k = """
            CREATE KEYSPACE IF NOT EXISTS {} WITH
            REPLICATION =
            {{ 'class' : 'SimpleStrategy',
              'replication_factor' : 1 }};""".format(self.key_space)
        self.cassandra.execute(k)
        s = """
            CREATE TABLE IF NOT EXISTS {} (
            key text,
            range_key int,
            data blob,
            PRIMARY KEY (key, range_key)
            )""".format(self.table_name)
        self.cassandra.execute(s)

    def _dropTable(self):
        k = """
            DROP KEYSPACE IF EXISTS {};""".format(self.key_space)
        self.cassandra.execute(k)
        s = """
            DROP TABLE IF EXISTS {};""".format(self.table_name)
        self.cassandra.execute(s)

    def _to_item(self, key, data):
        return Item.from_db_data(key, data)

    def _from_item(self, item):
        return {"key": item.key,
                "range_key": item.range_key,
                "data": item.to_string()}

    def _insert(self, key, range_key, data):
        s = """
            INSERT INTO {} (key, range_key, data)
            VALUES (%s, %s, %s)
            """.format(self.table_name)
        data = bytearray(data)
        self.cassandra.execute(s, (key, range_key, data))

    def _get(self, key, range_key):
        s = """
            SELECT key, range_key, data FROM {}
            WHERE key = %s AND range_key = %s
            """.format(self.table_name)
        res = self.cassandra.execute(s, (key, range_key))
        res = list(res)
        if len(res) < 1:
            raise NotFoundError
        return res[0].data

    def _first(self, key):
        s = """
            SELECT key, range_key, data FROM {}
            WHERE key = %s ORDER BY range_key ASC LIMIT 1
            """.format(self.table_name)
        res = self.cassandra.execute(s, (key, ))
        res = list(res)
        if len(res) < 1:
            raise NotFoundError
        return res[0].data

    def _last(self, key):
        s = """
            SELECT key, range_key, data FROM {}
            WHERE key = %s ORDER BY range_key DESC LIMIT 1
            """.format(self.table_name)
        res = self.cassandra.execute(s, (key, ))
        res = list(res)
        if len(res) < 1:
            raise NotFoundError
        return res[0].data

    def _left(self, key, range_key):
        s = """
            SELECT key, range_key, data FROM {}
            WHERE key = %s AND range_key <= %s ORDER BY range_key DESC LIMIT 1
            """.format(self.table_name)
        res = self.cassandra.execute(s, (key, range_key))
        res = list(res)
        if len(res) < 1:
            raise NotFoundError
        return res[0].data

    def _update(self, key, range_key, data):
        self._insert(key, range_key, data)

    def _query(self, key, range_min, range_max):
        s = """
            SELECT key, range_key, data FROM {}
            WHERE key = %s AND range_key >= %s AND range_key <= %s
            ORDER BY range_key ASC
            """.format(self.table_name)
        res = self.cassandra.execute(s, (key, range_min, range_max))
        items = []
        for r in res:
            items.append(r.data)
        try:
            left = self._left(key, range_min)
        except NotFoundError:
            return items
        if len(items) > 0 and left == items[0]:
            pass
        else:
            items.insert(0, left)
        return items


class SQLiteStorage(Storage):
    def __init__(self, filepath):
        self.filepath = filepath
        self.table_name = "datatable"
        import sqlite3
        self.conn = sqlite3.connect(filepath)

    def _createTable(self):
        c = self.conn.cursor()
        s = """
            CREATE TABLE IF NOT EXISTS {} (
            key text,
            range_key int,
            data blob,
            PRIMARY KEY (key, range_key)
            )""".format(self.table_name)
        c.execute(s)
        self.conn.commit()

    def _dropTable(self):
        c = self.conn.cursor()
        s = """
            DROP TABLE IF EXISTS {};""".format(self.table_name)
        c.execute(s)
        self.conn.commit()

    def _to_item(self, key, data):
        return Item.from_db_data(key, data)

    def _from_item(self, item):
        return {"key": item.key,
                "range_key": item.range_key,
                "data": item.to_string()}

    def _insert(self, key, range_key, data):
        s = """
            INSERT INTO {} (key, range_key, data)
            VALUES (?, ?, ?)
            """.format(self.table_name)
        data = buffer(data)
        c = self.conn.cursor()
        c.execute(s, (key, range_key, data))
        self.conn.commit()

    def _get(self, key, range_key):
        s = """
            SELECT key, range_key, data FROM {}
            WHERE key = ? AND range_key = ?
            """.format(self.table_name)
        c = self.conn.cursor()
        res = c.execute(s, (key, range_key))
        res = list(res)
        if len(res) < 1:
            raise NotFoundError
        return res[0][2]

    def _first(self, key):
        s = """
            SELECT key, range_key, data FROM {}
            WHERE key = ? ORDER BY range_key ASC LIMIT 1
            """.format(self.table_name)
        c = self.conn.cursor()
        res = c.execute(s, (key, ))
        res = list(res)
        if len(res) < 1:
            raise NotFoundError
        return res[0][2]

    def _last(self, key):
        s = """
            SELECT key, range_key, data FROM {}
            WHERE key = ? ORDER BY range_key DESC LIMIT 1
            """.format(self.table_name)
        c = self.conn.cursor()
        res = c.execute(s, (key, ))
        res = list(res)
        if len(res) < 1:
            raise NotFoundError
        return res[0][2]

    def _left(self, key, range_key):
        s = """
            SELECT key, range_key, data FROM {}
            WHERE key = ? AND range_key <= ? ORDER BY range_key DESC LIMIT 1
            """.format(self.table_name)
        c = self.conn.cursor()
        res = c.execute(s, (key, range_key))
        res = list(res)
        if len(res) < 1:
            raise NotFoundError
        return res[0][2]

    def _update(self, key, range_key, data):
        self._insert(key, range_key, data)

    def _query(self, key, range_min, range_max):
        s = """
            SELECT key, range_key, data FROM {}
            WHERE key = ? AND range_key >= ? AND range_key <= ?
            ORDER BY range_key ASC
            """.format(self.table_name)
        c = self.conn.cursor()
        res = c.execute(s, (key, range_min, range_max))
        items = []
        for r in res:
            items.append(r[2])
        try:
            left = self._left(key, range_min)
        except NotFoundError:
            return items
        if len(items) > 0 and left == items[0]:
            pass
        else:
            items.insert(0, left)
        return items


class RedisStorage(Storage):
    def __init__(self, redis=None, expire=None, **kwargs):
        if expire is not None:
            self.expire = expire
        else:
            self.expire = False
        if redis is not None:
            self.redis = redis
        else:
            self.redis = Redis(**kwargs)

    def _to_item(self, key, data):
        return Item.from_db_data(key, data)

    def _from_item(self, item):
        return {"key": item.key,
                "range_key": item.range_key,
                "data": item.to_string()}

    def _insert(self, key, range_key, data):
        self.redis.zadd(key, range_key, data)
        if self.expire:
            self.redis.expire(key, self.expire)

    def _get(self, key, range_key):
        l = self.redis.zrevrangebyscore(key, min=range_key, max=range_key,
                                        start=0, num=1)
        if len(l) < 1:
            raise NotFoundError
        return l[0]

    def _first(self, key):
        i = self.redis.zrangebyscore(key, min="-inf", max="+inf",
                                     start=0, num=1)
        if len(i) < 1:
            raise NotFoundError
        return i[0]

    def _last(self, key):
        i = self.redis.zrevrangebyscore(key, min="-inf", max="+inf",
                                        start=0, num=1)
        if len(i) < 1:
            raise NotFoundError
        return i[0]

    def _left(self, key, range_key):
        i = self.redis.zrevrangebyscore(key, min="-inf", max=range_key,
                                        start=0, num=1)
        if len(i) < 1:
            raise NotFoundError
        return i[0]

    def _update(self, key, range_key, data):
        p = self.redis.pipeline()
        p.zremrangebyscore(key, min=range_key, max=range_key)
        p.zadd(key, range_key, data)
        if self.expire:
            p.expire(key, self.expire)
        p.execute()

    def _query(self, key, range_min, range_max):
        items = self.redis.zrangebyscore(key, min=range_min, max=range_max)
        try:
            left = self._left(key, range_min)
        except NotFoundError:
            return items
        if len(items) > 0 and left == items[0]:
            pass
        else:
            items.insert(0, left)
        return items


class MemoryStorage(Storage):
    def __init__(self):
        self.cache = {}

    def _to_item(self, key, data):
        return Item.from_db_data(key, data)

    def _from_item(self, item):
        return {"key": item.key,
                "range_key": item.range_key,
                "data": item.to_string()}

    def _left(self, key, range_key):
        idx = self._le(key, range_key)
        return self._at(key, idx).data

    def _get_key(self, key):
        if key not in self.cache:
            self.cache[key] = list()
        return self.cache[key]

    def _get_range_keys(self, key):
        return [r.range_key for r in self._get_key(key)]

    def _index(self, key, range_key):
        a = self._get_range_keys(key)
        i = bisect.bisect_left(a, range_key)
        if i != len(a) and a[i] == range_key:
            return i
        raise NotFoundError

    def _ge(self, key, range_min):
        a = self._get_range_keys(key)
        i = bisect.bisect_left(a, range_min)
        return i

    def _le(self, key, range_max):
        a = self._get_range_keys(key)
        i = bisect.bisect_right(a, range_max)
        if i and i > 0:
            return i - 1
        raise NotFoundError

    def _at(self, key, index):
        return self._get_key(key)[index]

    def _slice(self, key, min, max):
        return self._get_key(key)[min:max]

    def _insert(self, key, range_key, data):
        a = self._get_range_keys(key)
        position = bisect.bisect_left(a, range_key)
        if position != len(a) and a[position] == range_key:
            raise ConflictError
        self._get_key(key).insert(position, Element(key, range_key, data))

    def _update(self, key, range_key, data):
        i = self._index(key, range_key)
        self._get_key(key)[i] = Element(key, range_key, data)

    def _get(self, key, range_key):
        i = self._index(key, range_key)
        return self._at(key, i).data

    def _query(self, key, range_min, range_max):
        m = self._ge(key, range_min)
        try:
            e = self._le(key, range_max) + 1
        except NotFoundError:
            return []
        # Get one before maybe there is a range key inside
        if m > 0:
            m -= 1
        return [x.data for x in self._slice(key, m, e)]

    def _last(self, key):
        k = self._get_key(key)
        if len(k) > 0:
            return k[-1].data
        raise NotFoundError

    def _first(self, key):
        k = self._get_key(key)
        if len(k) > 0:
            return k[0].data
        raise NotFoundError
