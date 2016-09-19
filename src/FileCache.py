#!/bin/env python

# https://gist.github.com/cfperez/59340ccf1dcb5eefdad5916366a13a5f
from pyspark.sql import DataFrame
from functools import wraps
import hashlib
from os import path, remove
import logging
from sys import stderr

def getLogger(name, debug=False, loglevel=None):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler(stderr))
    if debug or loglevel:
        level = loglevel or 'DEBUG'
        logger.setLevel(level)
    return logger

class FileCache:
    '''Store function results in a cache directory to avoid long-running functions.

    Very first attempt, used only to cache PySpark toPandas().

        ```
        cache = FileCache('cachedir', debug=True)

        @cache
        def long_running_func(arg):
            return arg
        ```
    '''
    def __init__(self, cachedir, debug=False, loglevel=None):
        if not path.isdir(cachedir):
            raise ValueError('cachedir must be a valid directory')
        self.cachedir = cachedir
        self.logger = getLogger(str(self.__class__), debug, loglevel)

    def __call__(self, func):
        return self.cache(func)

    def cache(self, func):
        'Function decorator to cache results'
        # TODO: This could be more robust
        func_cache = path.join(self.cachedir, func.__name__)
        @wraps(func)
        def _cached(*args):
            arg_hash = self._arg_hash(args)
            cache_file = path.join(func_cache, arg_hash)
            self.logger.debug('Checking for cached_file %s' % cache_file)
            if path.isfile(cache_file):
                self.logger.debug('>> Returning cached file')
                return pd.read_pickle(cache_file)
            self.logger.debug('>> Cache miss: running function %s()' % func.__name__)
            to_cache = func(*args)
            self.logger.debug('Attempting to cache...')
            try:
                pd.to_pickle(to_cache, cache_file)
            except:
                logger.error("Can't pickle funciton output!: %r" % to_cache)
                remove(cache_file)
            else:
                self.logger.debug('>> Cache successful!')
            return to_cache
        _cached.func_cache = func_cache
        return _cached

    def clear(self):
        raise NotImplemented

    @classmethod
    def _arg_hash(cls, args):
        arg_str = '|'.join(str(cls._expand_arg(arg)) for arg in args)
        return hashlib.md5(arg_str).hexdigest()

    @classmethod
    def _expand_arg(cls, arg):
        if isinstance(arg, DataFrame):
            return arg.schema
        else:
            return repr(arg)
