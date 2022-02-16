#!/usr/bin/env python
# -*- coding:utf-8 -*-

import copy
import logging
import os
import time

try:
    import MySQLdb
    import MySQLdb.constants
    import MySQLdb.converters
    import MySQLdb.cursors
except ImportError as err:
    raise ImportError(
        "Error loading MySQLdb module.\nDid you install mysqlclient?"
    ) from err

# pylint:disable=W1637


class Row(dict):
    """A dict that allows for object-like property access syntax."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


class Connection(object):
    def __init__(
        self,
        database,
        host="127.0.0.1",
        port=3306,
        user="root",
        password=None,
        max_idle_time=7 * 3600,
        connect_timeout=0,
        time_zone="+0:00",
        charset="utf8",
        sql_mode="TRADITIONAL",
        allow_ping=True,
        **kwargs
    ):
        """
        :param database: 数据库
        :param host: 数据库地址
        :param port: 端口
        :param user: 连接用户
        :param password: 密码
        :param max_idle_time: 最大空闲时间
        :param connect_timeout: 连接超时时间
        :param time_zone: 时区
        :param charset: 字符集
        :param sql_mode: 模式
        :param kwargs: 其他参数
        """
        self.host = host
        self.database = database
        self.max_idle_time = float(max_idle_time)

        db_kwargs = {
            "user": user,
            "passwd": password,
            "db": database,
            "conv": MySQLdb.converters.conversions,
            "use_unicode": True,
            "charset": charset,
            "init_command": ('SET time_zone = "%s"' % time_zone),
            "connect_timeout": connect_timeout,
            "sql_mode": sql_mode,
        }
        db_kwargs.update(kwargs)

        # 允许socket连接
        if "/" in host:
            db_kwargs["unix_socket"] = host
        else:
            db_kwargs["host"] = host
            db_kwargs["port"] = port

        self.db = None
        self.allow_ping = allow_ping
        self.__db_kwargs = db_kwargs
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to MySQL on %s", self.host, exc_info=True)

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "db", None) is not None:
            self.db.close()
            self.db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self.db = MySQLdb.connect(**self.__db_kwargs)
        self.db.autocommit(True)

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        self._ensure_connected()
        cursor = MySQLdb.cursors.SSCursor(self.db)
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def verify_ping(self):
        """为禁止ping的数据库提供ping功能"""
        cursor = self._cursor()
        try:
            self._execute(cursor, "SELECT 1", (), {})
        except Exception:
            return False
        return True

    def query(self, query, *parameters, **kwparameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    def get(self, query, *parameters, **kwparameters):
        """Returns the (singular) row returned by the given query.

        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = self.query(query, *parameters, **kwparameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    # rowcount is a more reasonable default return value than lastrowid,
    # but for historical compatibility execute() must return lastrowid.
    def execute(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *parameters, **kwparameters)

    def execute_lastrowid(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_rowcount(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the rowcount from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def executemany(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        return self.executemany_lastrowid(query, parameters)

    def executemany_lastrowid(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany_rowcount(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the rowcount from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    update = delete = execute_rowcount
    updatemany = executemany_rowcount

    insert = execute_lastrowid
    insertmany = executemany_lastrowid

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if self.db is None or (time.time() - self._last_use_time > self.max_idle_time):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self.db.cursor()

    def _execute(self, cursor, query, parameters, kwparameters):
        try:
            return cursor.execute(query, kwparameters or parameters)
        except MySQLdb.OperationalError:
            logging.error("Error connecting to MySQL on %s", self.host)
            self.close()
            raise
