#!/usr/bin/python
# coding: utf8
from __future__ import unicode_literals
import re
import logging

from .storage import MemoryStorage, RedisStorage
from .models import Item, ResultSet
from .errors import NotFoundError


logger = logging.getLogger(__name__)


class TSDB(object):
    def __init__(self, storage="memory", **kwargs):
        if storage == "memory":
            self.storage = MemoryStorage()
        elif storage == "redis":
            self.storage = RedisStorage()
        else:
            raise NotImplementedError("Storage not implemented")
        self.settings = {
            "BUCKETSIZE_TARGET": 100,
            "BUCKETSIZE_MAX": 200
        }
        self.settings.update(kwargs)
        Item.DYNAMICSIZE_TARGET = self.settings["BUCKETSIZE_TARGET"]
        Item.DYNAMICSIZE_MAX = self.settings["BUCKETSIZE_MAX"]

    def _get_last_item_or_new(self, key):
        try:
            item = self.storage.last(key)
        except NotFoundError:
            item = Item(key)
        return item

    def _get_items_between(self, key, ts_min, ts_max):
        return self.storage.query(key, ts_min, ts_max)

    def _query(self, key, ts_min, ts_max):
        r = ResultSet(key, self._get_items_between(key, ts_min, ts_max))
        r._trim(ts_min, ts_max)
        return r

    def _insert_or_update_item(self, item):
        if item.existing:
            self.storage.update(item)
        else:
            self.storage.insert(item)

    def _insert(self, key, data):
        key = key.lower()
        if not re.match(r'^[A-Za-z0-9_\-\.]+$', key):
            raise ValueError("Key should be alphanumeric (including .-_)")

        assert(isinstance(data, list))
        assert(len(data) > 0)
        data.sort(key=lambda x: x[0])

        # Limits and Stats
        ts_min = data[0][0]
        ts_max = data[-1][0]
        logger.debug("Inserting {} {} points".format(key, len(data)))
        logger.debug("Limits: {} - {}".format(ts_min, ts_max))
        stats = {"ts_min": ts_min, "ts_max": ts_max, "count": len(data),
                 "updated": 0, "splits": 0, "merged": 0, "key": key}

        # Find the last Item
        last_item = self._get_last_item_or_new(key)
        logger.debug("Last: {}".format(last_item))

        # List with all Items we updated
        updated = []

        # Just Append - Best Case
        if ts_min >= last_item.max_ts:
            logger.debug("Append Data")
            last_item.insert(data)
            updated.append(last_item)
        else:
            # Merge Round
            merge_items = self._get_items_between(key, ts_min, ts_max)
            assert(len(merge_items) > 0)
            assert(merge_items[0].min_ts <= ts_min)
            logger.debug("Merging Data Query({} - {}) {} items"
                         .format(ts_min, ts_max, len(merge_items)))
            i = len(data) - 1
            m = len(merge_items) - 1
            while i >= 0:
                last_merge_item = merge_items[m]
                if data[i][0] >= last_merge_item.min_ts:
                    last_merge_item.insert_point(data[i][0], data[i][1])
                    i -= 1
                else:
                    m -= 1
            updated += merge_items
            stats["merged"] += len(merge_items)

        # Splitting Round
        updated_splitted = []
        for i in updated:
            # Check Size for Split
            if not i.split_needed(limit="soft"):
                logger.debug("No Split, No Fragmentation")
                updated_splitted.append(i)
            # If its not the last we let it grow a bit
            elif i != last_item and not i.split_needed(limit="hard"):
                logger.debug("Fragmentation, No Split")
                updated_splitted.append(i)
            else:
                splited = i.split_item(self.settings["BUCKETSIZE_TARGET"])
                logger.debug("Split needed")
                for j in splited:
                    updated_splitted.append(j)
                stats["splits"] += 1

        # Update Round
        for i in updated_splitted:
            self._insert_or_update_item(i)
            stats["updated"] += 1

        logger.debug("Insert Finished {}".format(stats))
        return stats
