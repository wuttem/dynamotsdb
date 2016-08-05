#!/usr/bin/python
# coding: utf8

from __future__ import unicode_literals
import bisect
import logging
import itertools
import struct
import array


class Item(object):
    HEADER_SIZE = 4
    ITEMTYPES = ["dynamic", "hourly", "daily", "weekly"]
    DYNAMICSIZE_TARGET = 100
    DYNAMICSIZE_MAX = 190

    def __init__(self, key, values=None, version=1, item_type="dynamic"):
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

    def __eq__(self, other):
        if self.key != other.key:
            return False
        if self.version != other.version:
            return False
        if len(self._timestamps) != len(other._timestamps):
            return False
        if len(self._timestamps) > 0:
            if self._timestamps[0] != other._timestamps[0]:
                return False
            if self._timestamps[-1] != other._timestamps[-1]:
                return False
        return True

    def __ne__(self, other):
        return not self == other  # NOT return not self.__eq__(other)

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
        return -1

    @property
    def min_ts(self):
        if len(self._timestamps) > 0:
            return self._timestamps[0]
        return -1

    def split_needed(self, limit="soft"):
        if len(self) > Item.DYNAMICSIZE_MAX:
            return True
        if len(self) > Item.DYNAMICSIZE_TARGET and limit == "soft":
            return True
        return False

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


class ResultSet(Item):
    def __init__(self, key, items):
        super(ResultSet, self).__init__(key)
        for i in items:
            if i.key != key:
                raise ValueError("Item has wrong key")
            self._timestamps += i._timestamps
            self._values += i._values

    def _trim(self, ts_min, ts_max):
        low = bisect.bisect_left(self._timestamps, ts_min)
        high = bisect.bisect_right(self._timestamps, ts_max)
        self._timestamps = self._timestamps[low:high]
        self._values = self._values[low:high]

    def all(self):
        return itertools.izip(self._timestamps, self._values)
