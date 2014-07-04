from SocketServer import UnixStreamServer, StreamRequestHandler, ThreadingMixIn
from multiprocessing import Process, Value
import os
import select
import errno
from protocol import BaseIPCProtocol


__author__ = 'matheus2740'


class BaseIPCHandler(StreamRequestHandler):

    def handle(self):
        while 1:
            self.data = self.server.protocol.recover_message(self.request)

            if self.data == '__!goodbye__' or self.data is None:
                return

            if self.data == '__!shutdown__':
                self.server.harakiri()
                return

            fname = self.data['f']
            args = self.data['a']
            kwargs = self.data['kw']

            result = self.server._quiver[fname](*args, **kwargs)

            BaseIPCProtocol.send_message(self.request, [result])


class IPCAvailable(object):
    def __init__(self, ipc_server):
        self.server = ipc_server

    def __call__(self, func):

        self.server.register_functor(func)

        def wrapped(*args):
            result = func(*args)
            return result

        return wrapped


class BaseIPCServer(ThreadingMixIn, UnixStreamServer):
    """
    Threaded inter-process communication server. This server itself WON'T run in the process
    which initializes this class, it will run in a separate child process, so the initializer
    process (the one that instantiates this class) may do non related work.
    The child process opened, which is the server itself, will open a new thread for every client connected
    to it, and close the thread as soon as the client disconnects.

    To inherit from this, the following properties are noteworthy:
    :class attribute handler: The requisition handler class, defaults to BaseIPCHandler
     which is adequate for most scenarios.
    :class attribute protocol: The class in charge of serializing, deserializing, sending and
     retrieving information to and from the socket, defaults to BaseIPCProtocol which uses pickling.
    """
    daemon_threads = True
    request_queue_size = 128
    handler = BaseIPCHandler
    protocol = BaseIPCProtocol
    _quiver = {}

    def __init__(self, address='/tmp/BaseIPCServer.sock', start=True):
        """
        Initializes the server.
        :param address: (str) The unix socket file path
        :param start: (bool) Flag indicating if the server should startup rightaway.
        """
        self.address = address
        self.deleted_socket_file = False
        self.process = None
        self.shuttingdown = Value('i', 0)
        UnixStreamServer.__init__(self, self.address, self.handler)
        self._started = False
        if start:
            self.startup()

    @staticmethod
    def register_functor(functor, name=None):
        """
        Makes an functor available to client requests.
        If name is provided, the client will the functor through this else,
        functor.__name__ is used.
        Note: if you're registering a lambda expression make sure to pass the name argument, as lambdas are anonymous.
        :param functor: The functor to be registered.
        :param name: (optional) name which will be available to client calls.
        """
        BaseIPCServer._quiver[functor.__name__ if not name else name] = functor

    def shutdown(self):
        """
        Shuts down the server.
        """
        with self.shuttingdown.get_lock():
            self.shuttingdown.value = 1
        UnixStreamServer.server_close(self)
        if self.process:
            self.process.terminate()

        os.remove(self.address)
        self.deleted_socket_file = True

    def harakiri(self):
        """
        Shuts down the server from the server process itself.
        """
        with self.shuttingdown.get_lock():
            self.shuttingdown.value = 1
        UnixStreamServer.server_close(self)

        os.remove(self.address)
        self.deleted_socket_file = True

        exit(0)

    def __del__(self):
        """
        Delets the lock if shutdown wasn't called.
        """
        # os.remove(self.address)

    def startup(self):
        if self._started:
            return
        self.process = Process(target=self.serve_forever, args=(self.shuttingdown, 5))
        self.process.daemon = True
        self.process.start()
        self._started = True

    def serve_forever(self, shuttingdown, poll_interval=5):
        """Handle one request at a time until shutdown.

        Polls for shutdown every poll_interval seconds. Ignores
        self.timeout. If you need to do periodic tasks, do them in
        another thread.
        """
        while self.shuttingdown.value == 0:
            # XXX: Consider using another file descriptor or
            # connecting to the socket to wake this up instead of
            # polling. Polling reduces our responsiveness to a
            # shutdown request and wastes cpu at all other times.
            r, w, e = _eintr_retry(select.select, [self], [], [],
                                   poll_interval)
            if self in r:
                self._handle_request_noblock()


# This function is present on cpython but not pypy stdlib. I've added it here for pypy compatibility.
def _eintr_retry(func, *args):
    """restart a system call interrupted by EINTR"""
    while True:
        try:
            return func(*args)
        except (OSError, select.error) as e:
            if e.args[0] != errno.EINTR:
                raise