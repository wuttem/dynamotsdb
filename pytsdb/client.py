#!/usr/bin/python
# coding: utf8
from __future__ import unicode_literals
import re
import logging
import redis

from .storage import MemoryStorage, RedisStorage, CassandraStorage, SQLiteStorage
from .events import RedisPubSub
from .models import Item, ResultSet, BucketType, Stats
from .cache import RedisLRU
from .errors import NotFoundError


logger = logging.getLogger(__name__)


class TSDB(object):
    def __init__(self, STORAGE="memory", **kwargs):
        self.settings = {
            "BUCKET_TYPE": "dynamic",
            "BUCKET_DYNAMIC_TARGET": 100,
            "BUCKET_DYNAMIC_MAX": 200,
            "REDIS_PORT": 6379,
            "REDIS_HOST": "localhost",
            "REDIS_DB": 0,
            "SQLITE_FILE": "pytsdb.db3",
            "CASSANDRA_PORT": 9042,
            "CASSANDRA_HOST": "localhost",
            "ENABLE_CACHING": True,
            "ENABLE_EVENTS": True
        }
        self.settings.update(kwargs)

        # Setup Item Model
        Item.DYNAMICSIZE_TARGET = self.settings["BUCKET_DYNAMIC_TARGET"]
        Item.DYNAMICSIZE_MAX = self.settings["BUCKET_DYNAMIC_MAX"]
        Item.DEFAULT_BUCKETTYPE = BucketType[self.settings["BUCKET_TYPE"]]

        # Setup Redis Pool
        self.redis_pool = redis.ConnectionPool(host=self.settings["REDIS_HOST"],
                                               port=self.settings["REDIS_PORT"],
                                               db=self.settings["REDIS_DB"])

        # Setup Storage
        if STORAGE == "memory":
            self.storage = MemoryStorage()
        elif STORAGE == "sqlite":
            self.storage = SQLiteStorage(self.settings["SQLITE_FILE"])
        elif STORAGE == "redis":
            self.storage = RedisStorage(connection_pool=self.redis_pool)
        elif STORAGE == "cassandra":
            self.storage = CassandraStorage(
                contact_points=[self.settings["CASSANDRA_HOST"]],
                port=self.settings["CASSANDRA_PORT"])
        else:
            raise NotImplementedError("Storage not implemented")

        # Event Class
        if self.settings["ENABLE_EVENTS"]:
            self.events = RedisPubSub(connection_pool=self.redis_pool)

        if self.settings["ENABLE_CACHING"]:
            self.cache = RedisLRU(connection_pool=self.redis_pool)
            self.cache.setup_namespace("last_item", 1000)
            self.cache.setup_namespace("data_stats", 1000)
            self.cache.clearAll()

    def _register_data_listener(self, key, callback):
        if not self.settings["ENABLE_EVENTS"]:
            raise RuntimeError("Events not enabled")
        self.events.register_callback(key, callback)

    def _event(self, key, stats):
        if self.settings["ENABLE_EVENTS"]:
            self.events.publish_event(key=key,
                                      ts_min=stats["ts_min"],
                                      ts_max=stats["ts_max"],
                                      count=stats["count"],
                                      appended=stats["appended"],
                                      inserted=stats["inserted"],
                                      updated=stats["updated"],
                                      deleted=0)

    def _close(self):
        self.events.close()

    def _last_item_from_cache(self, key):
        # Cache Disbaled - Miss
        if not self.settings["ENABLE_CACHING"]:
            return None
        item_data = self.cache.get(key=key, namespace="last_item")
        if item_data is None:
            logger.debug("LAST GET MISS: {}".format(key))
            return None
        item = Item.from_db_data(key, item_data)
        logger.debug("LAST GET HIT: {}".format(item))
        return item

    def _store_last_item_in_cache(self, last_item):
        if not self.settings["ENABLE_CACHING"]:
            return
        logger.debug("LAST PUT: {}".format(last_item))
        self.cache.store(last_item.key, last_item.to_string(),
                         namespace="last_item")

    def _get_last_item_or_new(self, key):
        # Try to get it from Cache
        cached = self._last_item_from_cache(key)
        if cached is not None:
            return cached
        # Get it from DB
        try:
            item = self.storage.last(key)
        except NotFoundError:
            item = Item.new(key)
        else:
            self._store_last_item_in_cache(item)
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

    def _stats_from_cache(self, key):
        if not self.settings["ENABLE_CACHING"]:
            return None
        stats_raw = self.cache.get(key=key, namespace="data_stats")
        if stats_raw is None:
            logger.debug("STATS GET MISS: {}".format(key))
            return None
        stats = Stats.from_string(stats_raw)
        logger.debug("STATS GET HIT: {}".format(key))
        return stats

    def _stats(self, key):
        # Try to get the Stats from Cache
        cached = self._stats_from_cache(key)
        if cached is not None:
            return cached
        # From DB
        stats = self.storage.stats(key)
        self.cache.store(key, stats.to_string(), namespace="data_stats")
        return self.storage.stats(key)

    def _data_changed(self, key):
        if not self.settings["ENABLE_CACHING"]:
            return
        self.cache.expire(key, namespace="data_stats")

    def _insert(self, key, data):
        key = key.lower()
        if not re.match(r'^[A-Za-z0-9_\-\.]+$', key):
            raise ValueError("Key should be alphanumeric (including .-_)")

        assert(isinstance(data, list))
        assert(len(data) > 0)
        data.sort(key=lambda x: x[0])

        # Limits and Stats
        ts_min = int(data[0][0])
        ts_max = int(data[-1][0])
        count = len(data)
        logger.debug("Inserting {} {} points".format(key, len(data)))
        logger.debug("Limits: {} - {}".format(ts_min, ts_max))
        stats = {"ts_min": ts_min, "ts_max": ts_max, "count": count,
                 "appended": 0, "inserted": 0, "updated": 0, "key": key,
                 "splits": 0, "merged": 0}

        # Find the last Item
        last_item = self._get_last_item_or_new(key)
        if len(last_item) > 0:
            last_item_range_key = last_item.range_key
        else:
            last_item_range_key = -1
        logger.debug("Last: {}".format(last_item))

        # List with all Items we updated
        updated = []

        # Just Append - Best Case
        if ts_min >= last_item.ts_max:
            logger.debug("Append Data")
            appended = last_item.insert(data)
            updated.append(last_item)
            stats["appended"] += appended
        else:
            # Merge Round
            merge_items = self._get_items_between(key, ts_min, ts_max)
            assert(len(merge_items) > 0)
            assert(merge_items[0].ts_min <= ts_min)
            logger.debug("Merging Data Query({} - {}) {} items"
                         .format(ts_min, ts_max, len(merge_items)))
            i = len(data) - 1
            m = len(merge_items) - 1
            inserted = 0
            while i >= 0:
                last_merge_item = merge_items[m]
                if data[i][0] >= last_merge_item.ts_min:
                    inserted += last_merge_item.insert_point(data[i][0],
                                                             data[i][1])
                    i -= 1
                else:
                    m -= 1
            updated += merge_items
            stats["merged"] += len(merge_items)
            stats["inserted"] += inserted

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
                splited = i.split_item()
                logger.debug("Split needed")
                for j in splited:
                    updated_splitted.append(j)
                stats["splits"] += 1

        # Update
        if stats["inserted"] > 0 or stats["appended"] > 0:
            # Update Round
            for i in updated_splitted:
                self._insert_or_update_item(i)

            # Update Event
            self._event(key=key, stats=stats)
            logger.debug("Insert Finished {}".format(stats))

            # Invalidate Stats
            self._data_changed(key)

            # If it was the Last Item we update the Cache
            if updated_splitted[-1].range_key >= last_item_range_key:
                self._store_last_item_in_cache(updated_splitted[-1])
        else:
            logger.info("Duplicate ... Nothing to do ...")

        return stats
