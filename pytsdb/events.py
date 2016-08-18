#!/usr/bin/python
# coding: utf8

from __future__ import unicode_literals

import logging
import json
import sys
from redis import StrictRedis as Redis
from .errors import InternalError

logger = logging.getLogger(__name__)


class DataEvent(object):
    def __init__(self, key, ts_min, ts_max, count,
                 appended=0, inserted=0, updated=0, deleted=0):
        self.key = key
        self.ts_min = ts_min
        self.ts_max = ts_max
        self.count = count
        self.appended = appended
        self.inserted = inserted
        self.updated = updated
        self.deleted = deleted

    def to_dict(self):
        return {
            "key": self.key,
            "ts_min": self.ts_min,
            "ts_max": self.ts_max,
            "count": self.count,
            "appended": self.appended,
            "inserted": self.inserted,
            "updated": self.updated,
            "deleted": self.deleted,
        }

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, s):
        d = json.loads(s)
        return cls(**d)

    def __repr__(self):
        return "Event({}, count: {} timestamps: {} - {}".format(self.key,
                                                                self.count,
                                                                self.ts_min,
                                                                self.ts_max)


class RedisPubSub(object):
    def __init__(self, redis=None, **kwargs):
        if redis is not None:
            self._redis = redis
        else:
            self._redis = Redis(**kwargs)
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        self._thread = None
        self._callbacks = {}
        self._last_error = None

    def start(self):
        if not self._thread:
            self._thread = self._pubsub.run_in_thread(sleep_time=0.001)
            # TODO change in next version
            # self._thread = self._pubsub.run_in_thread(sleep_time=0.001,
            #                                          daemon=True)

    def stop(self):
        if self._thread is not None:
            self._thread.stop()
            self._thread = None

    def close(self):
        self.stop()
        self._pubsub.close()

    def publish_event(self, key, **kwargs):
        key = "{}".format(key)
        ev = DataEvent(key=key, **kwargs)
        self._redis.publish(key, ev.to_json())

    def register_callback(self, key, callback):
        key = "{}".format(key)
        self._callbacks[key] = callback
        self._pubsub.psubscribe(**{key: self._route_callback})
        self.start()

    def _route_callback(self, message):
        logger.info("Incomming Event: {}".format(message))
        pattern = message["pattern"].decode("utf-8")
        if pattern in self._callbacks:
            try:
                event = DataEvent.from_json(message["data"].decode("utf-8"))
                key = message["channel"].decode("utf-8")
                self._callbacks[pattern](key=key, event=event)
            except Exception as e:
                # TODO Python 3 Exception chaining
                t, value, traceback = sys.exc_info()
                self._last_error = "{}\n{}\n{}".format(t, value, traceback)
                logger.error(self._last_error)
                # Do not raise
                # raise InternalError, ("callback raised exception"), traceback
        else:
            raise InternalError("no callback found")
