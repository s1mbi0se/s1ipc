from s1ipc import BaseIPCClient

__author__ = 'salvia'


class SharedCache(object):
    """
    Dictionary-like class for caching objects, shared along processes.
    This class will Connect to a SharedCacheServer which must be initialized and started,
    or an exception will be raised.
    """

    def __init__(self, namespace, max_items=100, global_expiry=60 * 5,
                 autoclean=True, unlimited=False, address='/tmp/SharedCacheServer.sock'):
        """
        Initializes a new SharedCache object, which is a client for the SharedCacheServer.
        For other parameters please refer to the `Namespace` Class.
        :param address: The UNIX socket path which the server is listening.
        """
        self.namespace = namespace
        self.client = BaseIPCClient(address)
        self.client.configure_namespace(namespace, max_items, global_expiry, autoclean, unlimited)

    def __getitem__(self, item):
        """
        Gets an item from the cache.
        Raises KeyError if the item does not exist.
        :param item: The key to get.
        :return: The value for teh given key.
        :raise KeyError: If the item does not exist.
        """
        val = self.client.get(self.namespace, item)
        if val == u'!___null___':
            raise KeyError
        return val

    def get(self, item):
        """
        Gets an item from the cache.
        Returns None if the item does not exist.
        :param item: The key to get.
        :return: The value for teh given key, or None if it does not exist.
        """
        try:
            return self[item]
        except KeyError:
            return None

    def __setitem__(self, key, value):
        """
        Sets an item into the cache.
        :param key: The key of the item.
        :param value: The value of the Item.
        """
        self.client.put(self.namespace, key, value)

    def disconnect(self):
        """
        Disconnects from the SharedCacheServer.
        """
        self.client.disconnect()