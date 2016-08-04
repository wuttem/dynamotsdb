#!/usr/bin/python
# coding: utf8

from __future__ import unicode_literals
import bisect
from collections import namedtuple
from .errors import NotFoundError, ConflictError
from .models import Item


Element = namedtuple('Element', ['key', 'range_key', 'data'])


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
        # Get one before maybe there is a range key inside
        if m > 0:
            m -= 1
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
