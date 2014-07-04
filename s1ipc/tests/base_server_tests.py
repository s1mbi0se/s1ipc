# coding=utf-8
import hashlib
from multiprocessing import Pool
import unittest
from unittest import TestCase
from echoback_server import EchobackIPCServer, EchobackIPCClient
from .. import IPCAvailable

__author__ = 'matheus2740'

from .. import BaseIPCServer, BaseIPCClient


class IPCServerTests(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_echoback_server(self):
        server = EchobackIPCServer()
        try:
            echoback_client_call(123)
        finally:
            server.shutdown()
        server = EchobackIPCServer()
        try:
            for i in range(50):
                echoback_client_call("abc"+str(i))
        finally:
            server.shutdown()

    def test_echoback_server_parallel(self):
        server = EchobackIPCServer()


        pool = Pool(10)

        for i in range(1000):
            pool.apply_async(echoback_client_call, args=(i,))

        print 'applyed'
        pool.close()
        pool.join()
        print "pool is dead"

        server.shutdown()

    def test_hash_server(self):
        server = BaseIPCServer()

        try:
            data = "my test string"

            client = BaseIPCClient()

            received = client.mhash(data)
            print 'received:', received
            assert received == mhash(data)
        finally:
            server.shutdown()


@IPCAvailable(BaseIPCServer)
def mhash(string):
    return hashlib.md5(string).hexdigest()


def echoback_client_call(arg):
    client = EchobackIPCClient()

    received = client.teste(arg)

    assert received['a'][0] == arg
    print "successfull:", arg
    client.disconnect()

if __name__ == '__main__':
    unittest.main()