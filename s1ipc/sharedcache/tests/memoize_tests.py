# coding=utf-8
from multiprocessing import Process
import unittest
from unittest import TestCase
from .. import SharedCacheServer, SharedCache, Memoize
import time

__author__ = 'matheus2740'


class MemoizeTests(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        fun.remove_local_namespaces()

    def test_simple_call(self):
        server = SharedCacheServer()

        i1 = fun()
        i2 = fun()
        i2 = fun()
        i2 = fun()
        i2 = fun()

        assert i1 == i2
        assert fun.get_stats()['hits'] == 4
        assert fun.get_stats()['puts'] == 1
        assert fun.get_stats()['gets'] == 5

        server.shutdown()

    def test_parallel_call(self):
        server = SharedCacheServer()

        i1 = fun()

        def verify():
            i2 = fun()
            assert i1 == i2

        p = Process(target=verify)
        p.start()
        p.join()
        assert fun.get_stats()['hits'] == 1
        assert fun.get_stats()['puts'] == 1
        assert fun.get_stats()['gets'] == 2
        #time.sleep(0.5)
        server.shutdown()


@Memoize('test', 60)
def fun(*args):
    return 1

if __name__ == '__main__':
    unittest.main()