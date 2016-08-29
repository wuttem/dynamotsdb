#!/usr/bin/python
# coding: utf8

from os import environ
import logging
from .client import TSDB
from flask import (
    _app_ctx_stack as stack,
)

logger = logging.getLogger(__name__)


class FlaskTSDB(object):
    """TSDB Extension for Flask"""
    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_settings(self):
        """Initialize all of the extension settings."""
        self.app.config.setdefault('STORAGE', "cassandra")

        self.app.config.setdefault('REDIS_PORT', 6379)
        self.app.config.setdefault('REDIS_HOST', "localhost")
        self.app.config.setdefault('REDIS_DB', 0)

        self.app.config.setdefault('CASSANDRA_PORT', 9042)
        self.app.config.setdefault('CASSANDRA_HOST', "localhost")
        self.app.config.setdefault('ENABLE_CACHING', True)
        self.app.config.setdefault('ENABLE_EVENTS', True)

    def check_settings(self):
        """Check all user settings."""
        if not self.app.config["STORAGE"]:
            raise RuntimeError("Invalid Config (no storage engine)")

    def init_app(self, app):
        """
        Initialize this extension.

        :param obj app: The Flask application.
        """
        self.app = app
        self.init_settings()
        self.check_settings()

    @property
    def db(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'tsdb_connection'):
                ctx.tsdb_connection = TSDB(
                    STORAGE=self.app.config["STORAGE"],
                    REDIS_PORT=self.app.config["REDIS_PORT"],
                    REDIS_HOST=self.app.config["REDIS_HOST"],
                    REDIS_DB=self.app.config["REDIS_DB"],
                    CASSANDRA_PORT=self.app.config["CASSANDRA_PORT"],
                    CASSANDRA_HOST=self.app.config["CASSANDRA_HOST"],
                    ENABLE_CACHING=self.app.config["ENABLE_CACHING"],
                    ENABLE_EVENTS=self.app.config["ENABLE_EVENTS"])
            return ctx.tsdb_connection
        raise RuntimeError("No Flask Context")

    def __getattr__(self, item):
        """Redirect function calls to db instance.
        """
        result = getattr(self.db, item)
        if callable(result):
            return result
        return getattr(self, item)
