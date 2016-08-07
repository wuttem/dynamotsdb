#!/usr/bin/python
# coding: utf8

from __future__ import unicode_literals
from enum import Enum
from collections import MutableSequence
from collections import namedtuple
import bisect
import logging
import itertools
import struct
import array


Aggregation = namedtuple('Aggregation', ['min', 'max', 'sum', 'count'])


class BucketType(Enum):
    dynamic = 1
    hourly = 2
    daily = 3
    weekly = 4
    monthly = 5
    resultset = 6


class ItemType(Enum):
    raw_float = 1
    raw_int = 2
    tuple_float_2 = 3
    tuple_float_3 = 4
    tuple_float_4 = 5
    basic_aggregation = 6


class TupleArray(MutableSequence):
    def __init__(self, data_type=b"f", tuple_size=2):
        if tuple_size < 2 or tuple_size > 20:
            raise ValueError("invalid tuple size (2-20)")
        super(TupleArray, self).__init__()
        self.data_type = data_type
        self.tuple_size = tuple_size
        self._arrays = [array.array(data_type) for i in range(tuple_size)]

    def __len__(self):
        return len(self._arrays[0])

    def __getitem__(self, ii):
        return tuple(item[ii] for item in self._arrays)

    def __delitem__(self, ii):
        for a in self._arrays:
            del a[ii]

    def __setitem__(self, ii, val):
        if len(val) != len(self._arrays):
            raise ValueError("tuple size incorrect")

        for i, v in enumerate(val):
            self._arrays[i][ii] = v
        return tuple(item[ii] for item in self._arrays)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<TupleArray {} x {}>".format(self.data_type, self.tuple_size)

    def insert(self, ii, val):
        if len(val) != len(self._arrays):
            raise ValueError("tuple size incorrect")

        for i, v in enumerate(val):
            self._arrays[i].insert(ii, v)

    def append(self, val):
        if len(val) != len(self._arrays):
            raise ValueError("tuple size incorrect")

        for i, v in enumerate(val):
            self._arrays[i].append(v)

    def tostring(self):
        return b"".join([x.tostring() for x in self._arrays])

    def fromstring(self, string):
        s = len(string) / len(self._arrays)
        for i, a in enumerate(self._arrays):
            f = i * s
            t = i * s + s
            a.fromstring(string[f:t])


class Item(object):
    HEADER_SIZE = 8
    DYNAMICSIZE_TARGET = 100
    DYNAMICSIZE_MAX = 190

    def __init__(self, key, values=None, item_type=ItemType.raw_float,
                 bucket_type=BucketType.dynamic):
        self._timestamps = array.array(b"I")
        if item_type == ItemType.raw_float:
            self._values = array.array(b"f")
        elif item_type == ItemType.raw_int:
            self._values = array.array(b"I")
        elif item_type == ItemType.tuple_float_2:
            self._values = TupleArray(b"f", 2)
        elif item_type == ItemType.tuple_float_3:
            self._values = TupleArray(b"f", 3)
        elif item_type == ItemType.tuple_float_4:
            self._values = TupleArray(b"f", 4)
        elif item_type == ItemType.basic_aggregation:
            self._values = TupleArray(b"f", 4)
        else:
            raise NotImplementedError("invalid item type")
        if values is not None:
            self.insert(values)
        self._dirty = False
        self.existing = False
        self.key = str(key).lower()
        self.item_type = item_type
        self.bucket_type = bucket_type

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
        if self.item_type != other.item_type:
            return False
        if self.bucket_type != other.bucket_type:
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
        return "<{} series({}), min_ts: {}, items: {}, buckets: {}>".format(
            self.key, l, m, self.item_type, self.bucket_type)

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
        if self.item_type == ItemType.basic_aggregation:
            return (self._timestamps[i], Aggregation(*self._values[i]))
        return (self._timestamps[i], self._values[i])

    def __getitem__(self, key):
        return self._at(key)

    def to_list(self):
        out = list()
        for i in range(len(self._timestamps)):
            out.append(self._at(i))
        return out

    def to_string(self):
        header = (struct.pack("H", int(self.item_type.value)) +
                  struct.pack("H", int(self.bucket_type.value)))
        length = struct.pack("I", len(self))
        return (header + length + self._timestamps.tostring() +
                self._values.tostring())

    def split_item(self, count):
        if count >= len(self._timestamps):
            raise ValueError("split to big")
        splits = range(count, len(self._timestamps), count)
        splits += [len(self._timestamps)]

        new_items = []
        for s in range(len(splits) - 1):
            i = Item(self.key, item_type=self.item_type,
                     bucket_type=self.bucket_type)
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
        item_type = ItemType(int(struct.unpack("H", string[0:2])[0]))
        bucket_type = BucketType(int(struct.unpack("H", string[2:4])[0]))
        item_length = int(struct.unpack("I", string[4:8])[0])
        split = 8 + 4 * item_length
        ts, v = string[8:split], string[split:]
        i = Item(key, item_type=item_type, bucket_type=bucket_type)
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
        self.bucket_type = BucketType.resultset
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
