import json
import socket
from ..protocol import BaseIPCProtocol
from ..client import BaseIPCClient, retry_on_refuse, Caller
from ..server import BaseIPCHandler, BaseIPCServer

__author__ = 'matheus2740'

class EchobackIPCHandler(BaseIPCHandler):

    def handle(self):
        self.data = self.server.protocol.recover_message(self.request)

        BaseIPCProtocol.send_message(self.request, self.data)

class EchobackIPCServer(BaseIPCServer):
    handler = EchobackIPCHandler



class EchobackCaller(Caller):
    def parse(self, result):
        return result

class EchobackIPCClient(BaseIPCClient):
    def __getattr__(self, item):
        return EchobackCaller(self, item)

    def __del__(self):
        pass