
import importlib
import logging
import threading
import time
import uuid

import msgpack
import zmq

from .exceptions import Timeout, UnknownFormat, RemoteException, UnknownMessageType

LOGGER = logging.getLogger("zmqdrpc-client")
LOGGER.setLevel("WARNING")
_ = logging.StreamHandler()
_.setLevel('WARNING')
_.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
LOGGER.addHandler(_)

def gevent_patch():
    global zmq
    zmq = importlib.import_module('zmq.green')

class Replay():
    def __init__(self, uid, timeout_at):
        self.event = threading.Event()
        self.timeout_at = timeout_at
        self.uid = uid

    def get(self):
        now = time.time()
        if self.poll():
            return self.value
        if self.timeout_at <= now:
            raise Timeout("timeout")
        if self.event.wait(self.timeout_at - now):
            if self.is_exception:
                raise self.value
            return self.value
        else:
            raise Timeout("timeout")

    def __set(self, value, is_exception=False):
        self.is_exception = is_exception
        self.value = value
        self.event.set()

    def poll(self):
        return self.event.isSet()

class Call():
    def __init__(self, name, client):
        self.client = client
        self.name = name

    def __call__(self, *args, **kwargs):
        if "_timeout" in kwargs:
            timeout = kwargs['_timeout']
            kwargs.pop('_timeout')
        else:
            timeout = self.client._timeout
        return self.client._on_call(self.name, timeout, args, kwargs)

class Client(object):
    def __init__(self, address, timeout=60, threaded=True):
        self.__context = zmq.Context(1)
        self._timeout = timeout
        self.__address = tuple(address)
        self.__threaded = threaded
        if threaded:
            self.__tlocal = threading.local()
        else:
            class _(object):
                pass
            self.__tlocal = _()

    @property
    def __socket(self):
        if hasattr(self.__tlocal, "socket"):
            return self.__tlocal.socket
        else:
            self.__tlocal.socket = self.__context.socket(zmq.REQ)
            self.__tlocal.socket.connect("tcp://%s:%s"%self.__address)
            if not hasattr(self.__tlocal, "poller"):
                self.__tlocal.poller = zmq.Poller()
            self.__tlocal.poller.register(self.__tlocal.socket, zmq.POLLIN)
            return self.__tlocal.socket

    @__socket.setter
    def __socket(self, value):
        self.__tlocal.socket = value

    @property
    def __poller(self):
        if hasattr(self.__tlocal, "poller"):
            return self.__tlocal.poller
        else:
            self.__tlocal.poller = zmq.Poller()

    def __on_timeout(self):
        self.__poller.unregister(self.__socket)
        self.__socket.setsockopt(zmq.LINGER, 0)
        self.__socket.close()
        self.__socket = self.__context.socket(zmq.REQ)
        self.__socket.connect("tcp://%s:%s"%self.__address)
        self.__poller.register(self.__socket, zmq.POLLIN)

    def _on_call(self, name, timeout, args, kwargs):
        msg = msgpack.packb(["request", '', name, args, kwargs], encoding="utf-8")
        self.__socket.send_multipart([msg])
        socks = self.__poller.poll(timeout*1000)
        if socks:
            frames = self.__socket.recv_multipart()
            if len(frames) == 1:
                msg = msgpack.unpackb(frames[0], encoding="utf-8")
            else:
                raise UnknownFormat("unknow format")
            if msg[0] == "replay":
                return msg[2]
            elif msg[0] == "exception":
                raise RemoteException("{0}".format(msg[2]))
            else:
                raise UnknownMessageType("unknow message type")
        else:
            #if timeout we have to close the old one and create a new one
            #TODO:(cd)according to the zmq's doc, it's a bad behaviour to create and close lots of sockets
            self.__on_timeout()
            raise Timeout("timeout")

    def __close(self):
        self.__poller.unregister(self.__socket)
        self.__socket.setsockopt(zmq.LINGER, 0)
        self.__socket.close()
        self.__context.term()

    def __getattr__(self, name):
        return Call(name, self)

class AsyncClient(object):
    def __init__(self, address, timeout=60, threaded=True):
        self.__exit_flag = threading.Event()
        self.__address = tuple(address)
        self._timeout = timeout
        self.__uid = uuid.uuid1().hex
        self.__context = zmq.Context(1)
        self.__io_thread = threading.Thread(target=self.__io)
        self.__io_thread.daemon = True
        self.__io_thread.start()
        self.__replays = {}
        self.__threaded = threaded
        if threaded:
            self.__tlocal = threading.local()
        else:
            class _(object):
                pass
            self.__tlocal = _()

    @property
    def __push_socket(self):
        if hasattr(self.__tlocal, "socket"):
            return self.__tlocal.socket
        else:
            self.__tlocal.socket = self.__context.socket(zmq.PUSH)
            self.__tlocal.socket.connect("inproc://zmqdrpc-%s"%self.__uid)
            return self.__tlocal.socket

    @__push_socket.setter
    def __push_socket(self, value):
        self.__tlocal.socket = value

    def _on_call(self, name, timeout, args, kwargs):
        #TODO:(cd)make request id short
        request_id = uuid.uuid1().hex
        msg = msgpack.packb(["request", request_id, name, args, kwargs], encoding="utf-8")
        replay = Replay(request_id, time.time() + timeout)
        self.__replays[request_id] = replay
        self.__push_socket.send_multipart([msg])
        return replay

    def __io(self):
        self.__pull_socket = self.__context.socket(zmq.PULL)
        self.__pull_socket.bind("inproc://zmqdrpc-%s"%self.__uid)
        self.__socket = self.__context.socket(zmq.DEALER)
        self.__socket.connect("tcp://%s:%s"%self.__address)
        self.__poller = zmq.Poller()
        self.__poller.register(self.__pull_socket, zmq.POLLIN)
        self.__poller.register(self.__socket, zmq.POLLIN)
        while 1:
            socks = dict(self.__poller.poll(1000))
            if socks.get(self.__pull_socket) == zmq.POLLIN:
                frames = self.__pull_socket.recv_multipart()
                #add an empty frame to behave like REQ
                self.__socket.send_multipart([b''] + frames)
            if socks.get(self.__socket) == zmq.POLLIN:
                #skip the empty frame
                frames = self.__socket.recv_multipart()[1:]
                if len(frames) == 1:
                    msg = msgpack.unpackb(frames[0], encoding="utf-8")
                else:
                    LOGGER.warn("recv unknown format message")
                    continue
                if msg[0] == "replay":
                    rep = msg[2]
                    is_error = False
                elif msg[0] == "exception":
                    rep = RemoteException("{0}".format(msg[2]))
                    is_error = True
                else:
                    LOGGER.warn("unknow message type: %s", msg[0])
                    continue
                request_id = msg[1]
                if request_id in self.__replays:
                    self.__replays[request_id]._Replay__set(rep, is_error)
                    self.__replays.pop(request_id)
            now = time.time()
            self.__replays = dict((item for item in self.__replays.items() if now < item[1].timeout_at))
            if self.__exit_flag.isSet():
                break

    def __getattr__(self, name):
        return Call(name, self)
