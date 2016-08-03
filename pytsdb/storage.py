#!/usr/bin/python
# coding: utf8

from __future__ import unicode_literals
import bisect
import logging
import itertools
import struct
from collections import namedtuple
from .errors import NotFoundError, ConflictError
import array


Element = namedtuple('Element', ['key', 'range_key', 'data'])


class Item(object):
    HEADER_SIZE = 4

    def __init__(self, key, values=None, version=1):
        self._timestamps = array.array(b"I")
        self._values = array.array(b"f")
        if values is not None:
            self.insert(values)
        self._dirty = False
        self.existing = False
        self.key = str(key).lower()
        self.version = version

    @property
    def range_key(self):
        if len(self._timestamps) > 0:
            return self._timestamps[0]
        raise ValueError("empty series")

    def __len__(self):
        return len(self._timestamps)

    def is_sorted(self):
        it = iter(self._timestamps)
        it.next()
        return all(b >= a for a, b in itertools.izip(self._timestamps, it))

    def __bool__(self):
        if len(self._timestamps) != len(self._values):
            return False
        if not self.is_sorted():
            return False
        return True

    def __repr__(self):
        l = len(self._timestamps)
        if l > 0:
            m = self._timestamps[0]
        else:
            m = -1
        return "<{} series({}), min_ts: {}, version {}>".format(
            self.key, l, m, self.version)

    @property
    def max_ts(self):
        if len(self._timestamps) > 0:
            return self._timestamps[-1]
        return 0

    def min_ts(self):
        if len(self._timestamps) > 0:
            return self._timestamps[0]
        return 0

    def _at(self, i):
        return (self._timestamps[i], self._values[i])

    def __getitem__(self, key):
        return self._at(key)

    def to_list(self):
        out = list()
        for i in range(len(self._timestamps)):
            out.append(self._at(i))
        return out

    def to_string(self):
        header = struct.pack("I", self.version)
        return header + self._timestamps.tostring() + self._values.tostring()

    def split_item(self, count):
        if count >= len(self._timestamps):
            raise ValueError("split to big")
        splits = range(count, len(self._timestamps), count)
        splits += [len(self._timestamps)]

        new_items = []
        for s in range(len(splits) - 1):
            i = Item(self.key, version=self.version)
            i._dirty = True
            i._timestamps = self._timestamps[splits[s]:splits[s + 1]]
            i._values = self._values[splits[s]:splits[s + 1]]
            new_items.append(i)
        self._timestamps = self._timestamps[0:splits[0]]
        self._values = self._values[0:splits[0]]
        self._dirty = True

        new_items.insert(0, self)
        return new_items

    @classmethod
    def from_string(cls, key, string):
        version = int(struct.unpack("I", string[:4])[0])
        split = int(len(string) / 2) + 2
        ts, v = string[4:split], string[split:]
        i = Item(key, version=version)
        i._timestamps.fromstring(ts)
        i._values.fromstring(v)
        assert(i)
        return i

    @classmethod
    def from_db_data(cls, key, data):
        i = cls.from_string(key, data)
        i.existing = True
        return i

    def insert_point(self, timestamp, value, overwrite=False):
        timestamp = int(timestamp)
        value = float(value)
        idx = bisect.bisect_left(self._timestamps, timestamp)
        # Append
        if idx == len(self._timestamps):
            self._timestamps.append(timestamp)
            self._values.append(value)
            self._dirty = True
            return
        # Already Existing
        if self._timestamps[idx] == timestamp:
            # Replace
            logging.debug("duplicate insert")
            if overwrite:
                self._values[idx] = value
                self._dirty = True
            return
        # Normal Insert
        self._timestamps.insert(idx, timestamp)
        self._values.insert(idx, value)
        self._dirty = True

    def insert(self, series):
        for timestamp, value in series:
            self.insert_point(timestamp, value)


class MemoryStorage(object):
    def __init__(self):
        self.cache = {}

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
        return i

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
        return self._at(key, i)

    def _query(self, key, range_min, range_max):
        m = self._ge(key, range_min)
        e = self._le(key, range_max)
        return self._slice(key, m, e)

    def _last(self, key):
        k = self._get_key(key)
        if len(k) > 0:
            return k[-1]
        raise NotFoundError

    def _first(self, key):
        k = self._get_key(key)
        if len(k) > 0:
            return k[0]
        raise NotFoundError

    def get(self, key, range_key):
        return Item.from_db_data(key, self._get(key, range_key).data)

    def insert(self, item):
        self._insert(item.key, item.range_key, item.to_string())

    def update(self, item):
        self._update(item.key, item.range_key, item.to_string())

    def query(self, key, range_min, range_max):
        out = list()
        for i in self._query(key, range_min, range_max):
            out.append(Item.from_db_data(key, i.data))
        return out

    def last(self, key):
        return Item.from_db_data(key, self._last(key).data)

    def first(self, key):
        return Item.from_db_data(key, self._first(key).data)
