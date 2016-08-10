#!/usr/bin/python
# coding: utf8

from __future__ import unicode_literals

import logging
import json
from redis import StrictRedis as Redis


logger = logging.getLogger(__name__)


class RedisPubSub(object):
    def __init__(self, redis=None, **kwargs):
        if redis is not None:
            self._redis = redis
        else:
            self._redis = Redis(**kwargs)
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        self._thread = None
        self._callbacks = {}

    def start(self):
        if not self._thread:
            self._thread = self._pubsub.run_in_thread(sleep_time=0.001)
            # TODO change in next version
            # self._thread = self._pubsub.run_in_thread(sleep_time=0.001,
            #                                          daemon=True)

    def stop(self):
        if self._thread:
            self._thread.stop()
            self._thread = None

    def close(self):
        self.stop()
        self._pubsub.close()

    def new_data(self, key, range_min, range_max):
        self._redis.publish(key, json.dumps({"range_min": range_min,
                                             "range_max": range_max}))

    def register_callback(self, key, callback):
        self._callbacks[key] = callback
        self._pubsub.psubscribe(**{key: self._route_callback})
        self.start()

    def _route_callback(self, message):
        logger.info("Incomming Event: {}".format(message))
        pattern = message["pattern"]
        if pattern in self._callbacks:
            self._callbacks[pattern](message["channel"],
                                     info=json.loads(message["data"]))
        else:
            logger.warning("no callback found")
