#!/usr/bin/python
# coding: utf8
from __future__ import unicode_literals
import re
import logging

from .storage import MemoryStorage, Item
from .errors import NotFoundError


BUCKETSIZE_TARGET = 100
BUCKETSIZE_MAX = 190


logger = logging.getLogger(__name__)


class TSDB(object):
    def __init__(self, memory_storage=True, **kwargs):
        if memory_storage:
            self.storage = MemoryStorage()
        else:
            raise NotImplementedError("Storage not implemented")
        self.settings = {
            "BUCKETSIZE_TARGET": BUCKETSIZE_TARGET,
            "BUCKETSIZE_MAX": BUCKETSIZE_MAX
        }
        self.settings.update(kwargs)

    def _get_last_item_or_new(self, key):
        try:
            item = self.storage.last(key)
        except NotFoundError:
            item = Item(key)
        return item

    def _get(self):
        pass

    def insert_or_update_item(self, item):
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

        # Limits
        ts_min = data[0]
        ts_max = data[-1]

        # Find the last Item
        last_item = self._get_last_item_or_new(key)

        # List with all Items we updated
        updated = []

        # Just Append - Best Case
        if ts_min > last_item.max_ts:
            logger.info("Append Data")
            logger.debug("Before: %s" % last_item)
            last_item.insert(data)
            logger.debug("Before: %s" % last_item)
            updated.append(last_item)
        else:
            # Merge Round
            raise NotImplementedError("Merge needed")

        # Splitting Round
        updated_splitted = []
        for i in updated:
            # Check Size for Split
            if len(i) <= self.settings["BUCKETSIZE_TARGET"]:
                updated_splitted.append(i)
            # If its not the last we let it grow a bit
            elif i != last_item and len(i) <= self.settings["BUCKETSIZE_MAX"]:
                updated_splitted.append(i)
            else:
                splited = i.split_item(self.settings["BUCKETSIZE_TARGET"])
                for j in splited:
                    updated_splitted.append(j)

        # Update Round
        for i in updated_splitted:
            self.insert_or_update_item(i)

        logger.error("Updated: {}".format(updated_splitted))
