

import importlib
import msgpack
import zmq

from .exceptions import Timeout

def gevent_patch():
    global zmq
    zmq = importlib.import_module('zmq.green')

class Manager():
    def __init__(self, address, timeout=10):
        self.timeout = timeout
        self.address = address
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://%s:%s"%self.address)
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def status(self):
        self.socket.send_multipart([b"status"])
        socks = dict(self.poller.poll(self.timeout*1000))
        if socks.get(self.socket) == zmq.POLLIN:
            frames = self.socket.recv_multipart()
        else:
            self.close()
            self.socket = self.context.socket(zmq.REQ)
            self.socket.connect("tcp://%s:%s"%self.address)
            self.poller.register(self.socket, zmq.POLLIN)
            raise Timeout()
        return msgpack.unpackb(frames[0], encoding="utf-8")

    def close(self):
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.close()
        self.poller.unregister(self.socket)
