# -*- coding: utf-8  -*-
import select
import psycopg2
import psycopg2.extensions
import psycopg2.extras

from contextlib import contextmanager
import logging
from datetime import datetime
from trunk.utils import retry
import sys

import time

if sys.version_info[0] == 2:
    perf_counter = time.clock
else:
    perf_counter = time.perf_counter


try:
    from Queue import Empty
except ImportError:
    from queue import Empty  # noqa

LOG_FMT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger(__name__)


class LoggingCursor(psycopg2.extensions.cursor):
    """A cursor that logs queries using its connection logging facilities."""

    def execute(self, query, vars=None):
        ts = datetime.now()
        duration = 0
        begin_tran = False  # detect implicit transaction start
        try:
            old_tran_status = self.connection.get_transaction_status()
            start = perf_counter()
            res = super(LoggingCursor, self).execute(query, vars)
            duration = (perf_counter() - start) * 1000  # ms
            begin_tran = old_tran_status == 0 \
                         and self.connection.get_transaction_status() != 0
            return res
        finally:
            if begin_tran:
                _log_fmt = "--%s:\nBEGIN;" % str(ts)
                self.connection.log(_log_fmt, self)

            query_str = self.query.decode() \
                if isinstance(self.query, bytes) \
                else self.query
            _log_fmt = "--%s: %s\n%s;" % (str(ts), duration, query_str)
            self.connection.log(_log_fmt, self)


class LoggingConnection(psycopg2.extras.LoggingConnection):
    def __init__(self, *args, **kwargs):
        super(LoggingConnection, self).__init__(*args, **kwargs)
        self.initialize(logger)

    def cursor(self, *args, **kwargs):
        self._check()
        kwargs.setdefault('cursor_factory', LoggingCursor)
        return psycopg2.extensions.connection.cursor(self, *args, **kwargs)

    # It's not not possible to log transaction begin here: as per DBAPI spec,
    # a transaction is started implicitly with the first statement executed.

    def commit(self):
        do_log = self.get_transaction_status()
        super(LoggingConnection, self).commit()
        if do_log:
            _log_fmt = "--%s:\nCOMMIT;" % str(datetime.now())
            self.log(_log_fmt, None)

    def rollback(self):
        do_log = self.get_transaction_status()
        super(LoggingConnection, self).rollback()
        if do_log:
            _log_fmt = "--%s:\nROLLBACK;" % str(datetime.now())
            self.log(_log_fmt, None)


class Trunk(object):
    def __init__(self, dsn):
        self._dsn = dsn
        self.conn = self.connect(self._dsn)
        self.conn.autocommit = True

    @staticmethod
    def connect(dsn):
        return psycopg2.connect(dsn=dsn,
                                connection_factory=Trunk._conn_factory,
                                cursor_factory=LoggingCursor)

    @staticmethod
    def _conn_factory(*args, **kwargs):
        # connect, retry if R/O
        for _ in range(3):
            connection = LoggingConnection(*args, **kwargs)
            save_autocommit = connection.autocommit
            connection.autocommit = True  # don't start a transaction implicitly
            cursor = connection.cursor()  # type: psycopg2.extensions.cursor
            cursor.execute("SELECT pg_is_in_recovery()")
            is_ro = cursor.fetchone()
            cursor.close()
            connection.autocommit = save_autocommit
            if is_ro and is_ro[0]:
                logger.warning('Database is read-only, reconnecting')
                connection.close()
                continue
            return connection
        raise psycopg2._psycopg.OperationalError('Database is read-only')

    @contextmanager
    def cursor(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 'ping'")
        except:
            try:
                self.conn = retry(lambda: self.connect(self._dsn))
                self.conn.autocommit = True
                cursor = self.conn.cursor()
            except Exception:
                raise

        try:
            yield cursor
        except:
            raise
        finally:
            cursor.close()

    def listen(self, channel):
        try:
            with self.cursor() as cursor:
                cursor.execute("LISTEN \"%s\"" % channel)
        except Exception as e:
            logger.warning(str(e))

    def get(self, channel=None, block=True, timeout=None):
        if not block:
            timeout = 0
        while True:
            for notify in self.conn.notifies:
                if channel and notify.channel != channel:
                    continue
                self.conn.notifies.remove(notify)
                return notify.channel, notify.payload
            else:
                r, w, e = select.select([self.conn], [], [], timeout)
                if not (r or w or e):
                    raise Empty()
                self.conn.poll()

    def notify(self, channel, payload=None):
        try:
            with self.cursor() as cursor:
                cursor.execute("SELECT pg_notify(%s, %s);", (channel, payload))
        except Exception as e:
            logger.warning(str(e))

    def notifications(self, channel=None):
        while True:
            yield self.get(channel)

    def unlisten(self, channel):
        try:
            with self.cursor() as cursor:
                cursor.execute("UNLISTEN \"%s\";" % channel)
        except Exception as e:
            logger.warning(str(e))

    def channels(self):
        with self.cursor() as cursor:
            cursor.execute("SELECT pg_listening_channels();")
            channels = cursor.fetchall()
            return [c[0] for c in channels]

    def close(self):
        self.conn.close()
