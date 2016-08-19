#!/usr/bin/python
# coding: utf8

from __future__ import unicode_literals

import logging
import time
import json
from redis import StrictRedis as Redis

logger = logging.getLogger(__name__)


class RedisLRU(object):
    def __init__(self, redis=None, **kwargs):
        if redis is not None:
            self._redis = redis
        else:
            self._redis = Redis(**kwargs)
        self.namespaces = {
            "default": 10000
        }

    def setup_namespace(self, namespace, size):
        """Set the LRU Size for a namespace.
        """
        self.namespaces[namespace] = int(size)

    def _serialize(self, s):
        return json.dumps(s)

    def _unserialize(self, s):
        return json.loads(s)

    def _size(self, namespace):
        return self.namespaces[namespace]

    def _hit_store(self, namespace):
        if namespace not in self.namespaces:
            raise KeyError("invalid namespace")
        return "cache_keys_{}".format(namespace)

    def _value_store(self, namespace):
        if namespace not in self.namespaces:
            raise KeyError("invalid namespace")
        return "cache_values_{}".format(namespace)

    def _expire_old(self, namespace):
        hits = self._hit_store(namespace)
        size = self._size(namespace)
        count = self._redis.zcard(hits)
        logger.error("Expire {} >= {}".format(count, size))
        if count >= size:
            values = self._value_store(namespace)
            items = self._redis.zrange(hits, 0, count-size)
            logger.error(items)
            self._redis.zremrangebyrank(hits, 0, count-size)
            self._redis.hdel(values, *items)

    def clear(self, namespace="default"):
        """Clear the Cache.
        """
        hits = self._hit_store(namespace)
        values = self._value_store(namespace)
        self._redis.delete(hits, values)

    def store(self, key, value, namespace="default"):
        """Store a key value pair in cache.
        This will not update an existing item.
        """
        values = self._value_store(namespace)
        logger.error("store: {}".format(key))
        if not self._redis.hexists(values, key):
            hits = self._hit_store(namespace)
            self._expire_old(namespace)
            self._redis.hset(values, key, self._serialize(value))
            self._redis.zadd(hits, time.time(), key)

    def get(self, key, namespace="default"):
        """Get a value from the cache.
        returns none if the key is not found.
        """
        values = self._value_store(namespace)
        value = self._redis.hget(values, key)
        if value:
            hits = self._hit_store(namespace)
            self._redis.zadd(hits, time.time(), key)
            return self._unserialize(value.decode("utf-8"))
        return None

    def expire(self, key, namespace="default"):
        """Expire (invalidate) a key from the cache.
        """
        values = self._value_store(namespace)
        if self._redis.hexists(values, key):
            hits = self._hit_store(namespace)
            self._redis.hdel(values, key)
            self._redis.zrem(hits, key)
