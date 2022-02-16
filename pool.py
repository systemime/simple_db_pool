#!/usr/bin/env python
# -*- coding:utf-8 -*-

import contextlib
import logging
import warnings

from base_db import Connection, MySQLdb
from clean import observer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# 忽略 MySQLdb warnnings
warnings.filterwarnings(
    "ignore",
    category=MySQLdb.Warning,
)


class DatabaseError(Exception):
    pass


class TransactionAbortError(DatabaseError):
    pass


class ConnectionClosedError(DatabaseError):
    pass


class _Connector(object):
    """
    从池中取出链接的代理
    """

    def __init__(self, conn: Connection, pool, is_transaction=False):
        self.conn = conn
        self._pool = pool
        self._closed = False
        self.transaction_level = 0
        self.is_transaction = is_transaction
        if is_transaction:
            self.transaction_level += 1

    def __getattr__(self, name):
        if self._closed:
            raise ConnectionClosedError("Connection has closed")

        if self.conn:
            return getattr(self.conn, name)

    @property
    def in_transaction(self):
        return self.transaction_level > 0

    def close(self):
        """
        链接彻底关闭返回True，放回池返回False
        """
        # 如果连接池在事务中则不执行真正的关闭
        if self.in_transaction:
            return

        self._closed = True

        if self.conn not in self._pool._connections:
            self.conn.close()
            return True
        else:
            self._pool._idle_connections.append(self.conn)
            return False

    def __del__(self):
        if not self._closed:
            self.close()

    def __enter__(self):
        return self.conn

    def __exit__(self, type, value, traceback):
        if self.in_transaction and type is not None:
            self._pool.rollback_transaction()
        self.close()


class ConnectionPool:
    """数据库连接池, 如果没有超过连接数则创建连接, 并在使用完毕后将此连接放入池中,
    如果超过则创建一个临时连接, 在使用完毕后会关闭连接
    """

    def __init__(
        self,
        database,
        pool_size=10,
        host="127.0.0.1",
        port=3306,
        user="root",
        password=None,
        *args,
        **kwargs,
    ):
        self.pool_size = pool_size

        self._connections = []
        self._idle_connections = []
        self.pool_close = False
        self.database = database
        kwargs.update({"host": host, "port": port, "user": user, "password": password})
        self._args, self._kwargs = args, kwargs
        observer.add(self)

    # def __del__(self):
    #     if not self.pool_close:
    #         self.clean_pool()

    def _create(self) -> Connection:
        return Connection(self.database, *self._args, **self._kwargs)

    def _ping(self, conn, reconnect=True):

        if not conn.allow_ping:
            try:
                # 无法使用ping的数据库使用select 1的方式验证链接
                conn.db.verify_ping()
            except Exception:
                try:
                    conn.db.close()
                except Exception:
                    pass
                alive = False
            else:
                alive = True
        else:
            try:
                conn.db.ping()
            except Exception:
                logger.warning("当前数据库连接ping失败", exc_info=True)
                alive = False
            else:
                alive = True

        if not alive:
            if reconnect:
                try:
                    _conn = self._create()
                except Exception as err:
                    logger.error("数据库链接创建失败", exc_info=True)
                    raise err from err
                else:
                    if conn in self._connections:
                        self._connections.remove(conn)
                        self._connections.append(_conn)
                    conn = _conn
            else:
                raise ConnectionClosedError
        return conn

    def get_idle_connect(self):
        if self._idle_connections:
            conn = self._idle_connections.pop(0)
            try:
                _ = self._ping(conn)
            except Exception:
                logger.warning("空闲连接中存在关闭连接，已移除")
                return self.get_idle_connect()
        else:
            logger.warning("池中无空闲链接")
            return None
        return conn

    def connect(self, is_transaction=False):
        """连接数据库"""

        if self._idle_connections:
            conn = self.get_idle_connect()
            if conn:
                return _Connector(conn, self, is_transaction=is_transaction)

        conn = self._create()
        logger.info(f"创建新链接{conn}")

        current_pool_size = len(self._connections)
        if current_pool_size < self.pool_size:
            logger.debug(f"当前连接池容量为：{current_pool_size}/{self.pool_size}")
            self._connections.append(conn)
        else:
            logger.debug(f"链接池已满 {current_pool_size}/{self.pool_size}")

        return _Connector(conn, self, is_transaction=is_transaction)

    def clean_pool(self):
        """清空连接池"""
        self.pool_close = True

        for connect in self._idle_connections:
            connect.close()

        self._idle_connections = []
        self._connections = []

    def rollback_transaction(self, using_conn):
        """回滚事务"""
        with self._end_transaction(using_conn):
            if using_conn.conn.db is not None:
                logger.warning("事务回滚开始")
                using_conn.db.rollback()

            # 嵌套事务回滚回滚后抛出异常
            if using_conn.transaction_level > 1:
                using_conn.transaction_level = 0
                raise TransactionAbortError("当前事务已回滚，但整体事务未结束")

    @contextlib.contextmanager
    def _end_transaction(self, using_conn):
        if using_conn.transaction_level < 1:
            raise TransactionAbortError("未开启事务或已被关闭")

        using_conn.transaction_level -= 1
        try:
            yield
        finally:
            if using_conn.transaction_level < 1:
                using_conn.close()
                using_conn.is_transaction = False

    def commit_transaction(self, using_conn):
        """提交事务"""
        with self._end_transaction(using_conn):
            if using_conn.transaction_level > 1:
                logger.debug(f"处于嵌套事务中，当前事务层级 {using_conn.transaction_level}")
                return

            try:
                using_conn.conn.db.commit()
            except MySQLdb.OperationalError as err:  # pylint: disable=E1101
                using_conn.conn.db.rollback()
                raise err from err

    @contextlib.contextmanager
    def transaction_context(self, using_conn: _Connector = None):
        """事务"""
        # 若第一次开启事务则创建连接并开启事务

        if not using_conn:
            using_conn: _Connector = self.connect(is_transaction=True)
        else:
            try:
                # 嵌套事务链接中断则抛出异常
                self._ping(using_conn.conn)
            except Exception as err:
                logger.error("嵌套事务中断：sql链接断开！回滚取决于mysql配置")
                raise err from err
            using_conn.transaction_level += 1

        using_conn.conn.db.begin()
        logger.info(f"开启事务，当前事务层级: {using_conn.transaction_level}")

        try:
            yield using_conn
        except Exception as err:
            try:
                self.rollback_transaction(using_conn)
            except Exception as e:
                logger.exception("事务回滚异常", exc_info=e)
            raise err from err
        else:
            try:
                self.commit_transaction(using_conn)
            except Exception as err:
                logger.error("事务提交失败")
                raise err from err
