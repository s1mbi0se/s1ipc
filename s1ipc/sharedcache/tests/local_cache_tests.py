# coding=utf-8
from multiprocessing import Process
import unittest
from unittest import TestCase
from .. import SharedCacheServer, SharedCache

__author__ = 'matheus2740'



class LocalCacheTests(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple_call(self):
        server = SharedCacheServer()
        cache = SharedCache('test')
        key, value = 'testk', 'testv'
        cache[key] = value
        assert cache[key] == value
        server.shutdown()

    def test_parallel_call(self):
        server = SharedCacheServer()
        cache = SharedCache('test')
        key, value = 'testk', 'testv'

        def verify():
            cache = SharedCache('test')
            cache['testk'] = value
            assert cache[key] == value

        p = Process(target=verify)
        p.start()
        p.join()
        server.shutdown()


if __name__ == '__main__':
    unittest.main()