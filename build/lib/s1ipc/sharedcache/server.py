from collections import OrderedDict, deque
import threading
import time

__author__ = 'salvia'
from s1ipc import BaseIPCServer


_cache = {}


class Value(object):

    def __init__(self, value, insertion, expiry=(60 * 60 * 24)):
        self.value = value
        self.insertion = insertion
        self.expiry = expiry


class Namespace(object):
    """
    `Namespace` is a dictionary-like class proper for caching objects, supporting object count limit,
     expiry, automatic cleaning, and hit/put/get statistics.
    """

    def __init__(self, name, max_items=100, global_expiry=(60 * 5), autoclean=True, unlimited=False):
        """
        Initializes the namespace.
        :param name: (str) The name of the namespace.
        :param max_items: (int) The maximum number of items this namespace can hold.
        :param global_expiry: (int) The maximum time in seconds that this namespace will retain some object.
        :param autoclean: (bool) Flag indicating that this namespace should automatically remove expired objects.
        :param unlimited: (bool) Flag indicating that this namespace is unlimited, meaning it will any number of objects
        indefinitely (overrides max_items and global_expiry)
        """
        self.max_items = max_items
        self.global_expiry = global_expiry
        self.objects = OrderedDict()
        self.autoclean = autoclean
        self._lastclean = time.time()
        self.name = name
        self.unlimited = unlimited
        self._lock = threading.Lock()
        self.hits = 0
        self.gets = 0
        self.puts = 0

    def __getitem__(self, item):
        """
        Retrieves an item from the namespace.
        :param item: The key which the value is associated.
        :return: Return the value from the given key.
        :raise KeyError: If the key-value pair does not exist in this namespace.
        """
        hit = False
        try:
            val = self.objects[item]
            now = time.time()

            # Check if the object expired
            if not self.unlimited and now - val.insertion >= min(self.global_expiry, val.expiry):
                self.objects.pop(item)
                raise KeyError()
            hit = True
            return val.value
        finally:
            with self._lock:
                self.gets += 1
                if hit:
                    self.hits += 1

    def __setitem__(self, key, value):
        """
        Sets an item (key-value pair) in the namespace.
        :param key:
        :param value:
        """
        self.verify_and_clean()
        if 0 < self.max_items <= len(self.objects):
            delkey = next(iter(self.objects))
            self.objects.pop(delkey)
        value = Value(value[0], time.time(), value[1])
        with self._lock:
            self.objects[key] = value
            self.puts += 1

    def invalidate(self):
        """
        Invalidates all items in the namespace, effectively removing all data.
        """
        with self._lock:
            self.objects = OrderedDict()

    def verify_and_clean(self):
        """
        (non-blocking)
        Verify if a clean is necessary, and if is, opens a new thread to clean expired items.
        """
        now = time.time()
        if not self.unlimited and self.autoclean and now - self._lastclean >= self.global_expiry:
            self._lastclean = now
            t = threading.Thread(target=self.cleanup)
            t.start()

    def reset_stats(self):
        """
        Reset the hits/gets/puts counters.
        """
        self.hits = 0
        self.gets = 0
        self.puts = 0

    def get_stats(self):
        """
        Retrives the hits/gets/puts counters.
        :return: A dictionary str->int int following form:
        {
            'hits': `number of cache hits`,
            'gets': `number of cache gets (hits and misses)`,
            'puts': `number of cache insertions`
        }
        """
        return {
            'hits': self.hits,
            'gets': self.gets,
            'puts': self.puts
        }

    def cleanup(self):
        """
         (blocking)
         Remove expired items
        """
        if self.unlimited:
            return
        now = time.time()
        delete = deque()

        for k, v in self.objects.iteritems():
            if now - v.insertion >= min(self.global_expiry, v.expiry):
                delete.append(k)

        with self._lock:
            for d in delete:
                try:
                    self.objects.pop(d)
                except KeyError:
                    pass


class SharedCacheServer(BaseIPCServer):
    """
    IPC server designed for shared caching.
    """

    def __init__(self, address='/tmp/SharedCacheServer.sock', start=True):
        """
        Initializes the server.
        :param address: (str) The unix socket file path
        :param start: (bool) Flag indicating if the server should startup rightaway.
        """
        BaseIPCServer.__init__(self, address, start)

    @staticmethod
    def create_namespace(name, max_items=100, global_expiry=60 * 5, autoclean=True, unlimited=False):
        """
        Inserts a new namespace in this server.
        For parameters refer to the `Namespace` class.
        """
        if name in _cache:
            raise KeyError('Namespace already exists.')
        _cache[name] = Namespace(name, max_items, global_expiry, autoclean, unlimited)

    @staticmethod
    def configure_namespace(name, max_items=None, global_expiry=None, autoclean=None, unlimited=None):
        """
        Configures an existing namespace or creates a new one.
        For parameters refer to the `Namespace` class.
        """
        if name in _cache:
            namespace = _cache[name]
            if max_items is not None:
                namespace.max_items = max_items
            if global_expiry is not None:
                namespace.global_expiry = global_expiry
            if autoclean is not None:
                namespace.autoclean = autoclean
            if unlimited is not None:
                namespace.unlimited = unlimited
        else:
            _cache[name] = Namespace(name, max_items, global_expiry, autoclean, unlimited)

    @staticmethod
    def put(namespace, key, value, expiry=(60 * 60 * 24)):
        """
        Inserts an item into the cache.
        If the namespace does not exist, a new one with default configuration will be created.
        :param namespace: Name of the namespace.
        :param key: item key
        :param value: item value
        :param expiry: expiry in secondos for this item
        """
        if not namespace in _cache:
            _cache[namespace] = Namespace(namespace)

        _cache[namespace][key] = (value, expiry)

    @staticmethod
    def get(namespace, key):
        """
        Gets an item from the cache.
        Returns the special '!___null___' marker in case the value is not found.
        :param namespace: Name of the namespace.
        :param key: The key of item to get.
        :return: The item associated with the key and namespace, or '!___null___' in case it does not exist.
        """
        try:
            return _cache[namespace][key]
        except KeyError:
            return '!___null___'

    @staticmethod
    def invalidate(namespace):
        """
        Invalidates the namespace.
        :param namespace: Name of the namespace to invalidate.
        """
        if namespace in _cache:
            _cache[namespace].invalidate()

    @staticmethod
    def get_stats(namespace):
        """
        Retrive statistics for the given namespace.
        :param namespace: Name of the namespace.
        :return: Refer to `Namespace.get_stats`
        """
        if namespace in _cache:
            return _cache[namespace].get_stats()
        else:
            return '!___null___'

    @staticmethod
    def reset_stats(namespace):
        """
        Resets the statistics for the given namespace.
        :param namespace: Name of the namespace.
        :return:
        """
        if namespace in _cache:
            return _cache[namespace].reset_stats()
        else:
            return '!___null___'


# make the functions available for IPC calls
SharedCacheServer.register_functor(SharedCacheServer.create_namespace)
SharedCacheServer.register_functor(SharedCacheServer.configure_namespace)
SharedCacheServer.register_functor(SharedCacheServer.put)
SharedCacheServer.register_functor(SharedCacheServer.get)
SharedCacheServer.register_functor(SharedCacheServer.invalidate)
SharedCacheServer.register_functor(SharedCacheServer.reset_stats)
SharedCacheServer.register_functor(SharedCacheServer.get_stats)