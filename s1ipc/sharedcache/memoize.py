from collections import OrderedDict
from shared_cache import SharedCache
from time import time
import pickle

__author__ = 'matheus2740'

_function_cache = {}

class Wrapper(object):

    def __init__(self, memo, func):
        self.memo = memo
        self.func = func

    def get_stats(self):
        if not self.memo.namespace in _function_cache:
            _function_cache[self.memo.namespace] = SharedCache(self.memo.namespace, max_items=100000, global_expiry=self.memo.expiry_time)
        return _function_cache[self.memo.namespace].client.get_stats(self.memo.namespace)

    def reset_stats(self):
        if not self.memo.namespace in _function_cache:
            _function_cache[self.memo.namespace] = SharedCache(self.memo.namespace, max_items=100000, global_expiry=self.memo.expiry_time)
        return _function_cache[self.memo.namespace].client.reset_stats(self.memo.namespace)

    def remove_local_namespaces(self):
        global _function_cache
        _function_cache = {}

    def disconnect(self):
        for k, v in _function_cache.iteritems():
            v.disconnect()

    def __call__(self, *args, **kwargs):
        if not self.memo.namespace in _function_cache:
                _function_cache[self.memo.namespace] = SharedCache(self.memo.namespace, max_items=100000, global_expiry=self.memo.expiry_time)
        # The *args list will act as the cache key (at least the first part of it)
        # [:None] is equivalent to [:]
        mem_args = list(args[:self.memo.num_args]) + kwargs.items()
        # Get the name of the decorated function
        name = self.func.__name__
        key = pickle.dumps([name] + mem_args)
        # Check the cache
        try:
            result = _function_cache[self.memo.namespace][key]
            return result
        except KeyError:
            pass
            # Get a new result
        result = self.func(*args, **kwargs)
        # Cache it
        if self.memo.validator(result):
            _function_cache[self.memo.namespace][key] = result
        # and return it.
        return result


class ValidativeMemoize(object):
    """
    Memoization decorator interface for the SharedCache system,
    Supports validation of return values of the wrapped function, to determine
    if specific values should or not be cached.
    """

    def __init__(self, namespace, expiry_time=0, num_args=None, validator=lambda x: True):
        """
        Initializes a ValidativeMemoize.
        :param namespace: Name of the namespace to use in the SharedCacheServer.
        :param expiry_time: expiry in seconds for values returned by the wrapped function.
        :param num_args: Number of relevant arguments to use as cache kay. None means all of arguments are relevant.
        :param validator: functor of the form f(object):bool. Any value returned by the wrapped function will be submited
        to this functor. If it returns True, the value is cached.
        """
        self.namespace = namespace
        self.expiry_time = expiry_time
        self.num_args = num_args
        self.validator = validator

    def __call__(self, func):
        return Wrapper(self, func)


class NonNoneMemoize(ValidativeMemoize):
    """
    The same as ValidativeMemoize except the functor is fixed to `lambda x: x is not None`,
    id est, only cache non-None values.
    """
    def __init__(self, namespace, expiry_time=0, num_args=None):
        super(NonNoneMemoize, self).__init__(namespace, expiry_time, num_args, validator=lambda x: x is not None)


# Indexed memoization
class Memoize(ValidativeMemoize):
    """
    The same as ValidativeMemoize except the functor is fixed to `lambda x: True`,
    id est, cache all values.
    """
    def __init__(self, namespace, expiry_time=0, num_args=None):
        super(Memoize, self).__init__(namespace, expiry_time, num_args, validator=lambda x: True)


_l_function_cache = {}


class LocalValidativeMemoize(object):
    """
    Same functionality as ValidativeMemoize, but stores the objects locally meaning the cache is nor shared,
    nor it requires an SharedCacheServer to be running.
    """
    def __init__(self, namespace, expiry_time=0, num_args=None, validator=lambda x: True):
        """
        Initializes a LocalValidativeMemoize.
        :param namespace: Name of the namespace to use (locally, no SharedCacheServer is required).
        :param expiry_time: expiry in seconds for values returned by the wrapped function.
        :param num_args: Number of relevant arguments to use as cache kay. None means all of arguments are relevant.
        :param validator: functor of the form f(object):bool. Any value returned by the wrapped function will be submited
        to this functor. If it returns True, the value is cached.
        """
        if not namespace in _l_function_cache:
            _l_function_cache[namespace] = {}
        self.index = namespace
        self.expiry_time = expiry_time
        self.num_args = num_args
        self.validator = validator

    def __call__(self, func):
        def wrapped(*args):
            # The *args list will act as the cache key (at least the first part of it)
            # [:None] is equivalent to [:]
            mem_args = pickle.dumps(args[:self.num_args])
            # Get the name of the decorated function
            name = func.__name__
            # check if the function is already indexed
            if not name in _l_function_cache[self.index]:
                _l_function_cache[self.index][name] = OrderedDict({})
            # Check the cache
            if mem_args in _l_function_cache[self.index][name]:
                result, timestamp = _l_function_cache[self.index][name][mem_args]
                # Check the age.
                age = time() - timestamp
                if not self.expiry_time or age < self.expiry_time:
                    return result
                else:
                    _l_function_cache[self.index][name] = {}
                # Get a new result
            result = func(*args)
            # Cache it
            if self.validator(result):
                if len(_l_function_cache[self.index][name]) >= 225000:
                    _l_function_cache[self.index][name].pop(next(iter(_l_function_cache[self.index][name])))
                _l_function_cache[self.index][name][mem_args] = (result, time())
            # and return it.
            return result

        return wrapped


class LocalNonNoneMemoize(LocalValidativeMemoize):
    def __init__(self, namespace, expiry_time=0, num_args=None):
        super(LocalNonNoneMemoize, self).__init__(namespace, expiry_time, num_args, validator=lambda x: x is not None)


# Indexed memoization
class LocalMemoize(LocalValidativeMemoize):
    def __init__(self, namespace, expiry_time=0, num_args=None):
        super(LocalMemoize, self).__init__(namespace, expiry_time, num_args, validator=lambda x: True)
