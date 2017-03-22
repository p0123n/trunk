# -*- coding: utf-8  -*-
try:
    from urllib.parse import urlunparse
except ImportError:
    from urlparse import urlunparse  # noqa


def build_dsn(scheme='postgres', hostname='localhost', port=5432, path='', username=None, password=None):
    netloc = hostname or 'localhost'
    if port:
        netloc = "{0}:{1}".format(netloc, port)
    if username and password:
        netloc = "{0}:{1}@{2}".format(username, password, netloc)
    if username and not password:
        netloc = "{0}@{1}".format(username, netloc)
    return urlunparse((scheme, netloc, path, None, None, None))


def retry(func, attempts=3, sleep_sec=1, exception_class=Exception, onerror=None):
    from time import sleep

    while True:
        try:
            return func()
        except exception_class as ex:
            attempts -= 1
            if not attempts:  # failure
                raise
            if onerror is not None:
                onerror(ex)

            sleep(sleep_sec)
