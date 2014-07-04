__author__ = 'matheus2740'

import pickle


class ICPProtocolException(Exception):
    pass


class BaseIPCProtocol(object):
    """
    Class which handles how the data is passed through the socket.
    This base class implements a very simple mechanism of pickling objects and
    prefixing the message with `HEADER_SIZE` hexadecimal digits meaning the lenght of the message.
    If your server is to communicate objects which cannot be serialized by pickle, you must
    define your own protocol.
    If your server is to communicate messages whose lenght cannot be expressed within `HEADER_SIZE` hexadecimal
    digits, you should subclass this protocol and increase the `HEADER_SIZE` class variable.

    Any protocol must define the static methods: `pack_message`, `send_message` and `recover_message`.
    Note: the default `HEADER_SIZE` is 8 hexadecimal digits, which can describe a message of size up to 4GB.
    """

    HEADER_SIZE = 8

    @staticmethod
    def pack_message(data):
        """
        This method receives any object that is meant to be communicated from or to the server.
        This method should return a string, which will be passed through the socket.
        The BaseIPCProtocol will pickle and prefix this data with lenght.
        :param data: any object
        :return: a string which will be messaged through the socket.
        """

        data = pickle.dumps(data)

        header = hex(len(data))[2:]
        if len(header) > BaseIPCProtocol.HEADER_SIZE:
            raise ICPProtocolException('Attempted sending message too large for protocol: BaseIPCProtocol')

        elif len(header) < BaseIPCProtocol.HEADER_SIZE:
            zeros = '0' * (BaseIPCProtocol.HEADER_SIZE - len(header))
            header = zeros + header

        packet = header + data
        return packet

    @staticmethod
    def send_message(sock, data):
        """
        This method receives an socket object and raw data to be communicated.
        The BaseIPCProtocol will pickle and prefix this data with lenght.
        :param sock: a socket object
        :param data: any object
        """
        packet = BaseIPCProtocol.pack_message(data)

        sock.send(packet)
        # sock.flush()
        pass

    @staticmethod
    def recover_message(sock):
        """
        This method receives a socket object and must receive and parse the message from it.
        :param sock: a socket object
        :return: the parsed messagem into the original object
        """
        try:
            header = sock.recv(BaseIPCProtocol.HEADER_SIZE)
            length = int('0x'+header, 16)
            payload = sock.recv(length)
            return pickle.loads(payload)
        except ValueError:
            return None
