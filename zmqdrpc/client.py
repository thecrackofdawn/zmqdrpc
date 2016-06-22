import zmq
import msgpack
import exceptions
import threading
import time
import logging
import contextlib
import uuid

LOGGER = logging.getLogger("zmqdrpc-client")
LOGGER.setLevel("WARNING")
_ = logging.StreamHandler()
_.setLevel('WARNING')
_.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
LOGGER.addHandler(_)

class Replay():
    def __init__(self, uid, timeoutAt):
        self.event = threading.Event()
        self.timeoutAt = timeoutAt
        self.uid = uid

    def get(self):
        now = time.time()
        if self.poll():
            return self.value
        if self.timeoutAt <= now:
            raise Exception("timeout")
        if self.event.wait(self.timeoutAt - now):
            if self.isException:
                raise self.value
            return self.value
        else:
            raise Exception("timeout")

    def __set(self, value, isException=False):
        self.isException = isException
        self.value = value
        self.event.set()

    def poll(self):
        return self.event.isSet()

class Call():
    def __init__(self, name, client):
        self.client = client
        self.name = name

    def __call__(self, *args, **kwargs):
        return getattr(self.client, ''.join(['_', self.client.__class__.__name__, "__onCall"]))(self.name, args, kwargs)

@contextlib.contextmanager
def Timeout(client, timeout):
    orginTimeout = client._Client__timeout
    client._Client__timeout = timeout
    try:
        yield
    finally:
        client._Client__timeout = orginTimeout

class Client():
    """
    not threaded safe
    """
    def __init__(self, address, timeout=60):
        self.__uid = uuid.uuid1().hex
        self.__context = zmq.Context(1)
        self.__timeout = timeout
        self.__address = address
        self.__socket = self.__context.socket(zmq.REQ)
        self.__socket.connect("tcp://%s:%s"%self.__address)
        self.__poller = zmq.Poller()
        self.__poller.register(self.__socket, zmq.POLLIN)

    def __onTimeout(self):
        self.__poller.unregister(self.__socket)
        self.__socket.setsockopt(zmq.LINGER, 0)
        self.__socket.close()
        self.__socket = self.__context.socket(zmq.REQ)
        self.__socket.connect("tcp://%s:%s"%self.__address)
        self.__poller.register(self.__socket, zmq.POLLIN)

    def __onCall(self, name, args, kwargs):
        msg = msgpack.packb(["request", '', name, args, kwargs])
        self.__socket.send_multipart([msg])
        socks = self.__poller.poll(self.__timeout*1000)
        if socks:
            frames = self.__socket.recv_multipart()
            if len(frames) == 1:
                msg = msgpack.unpackb(frames[0])
            else:
                raise Exception("unknow format")
            if msg[0] == "replay":
                return msg[2]
            elif msg[0] == "Exception":
                try:
                    raise getattr(exceptions, msg[2])(msg[3])
                except AttributeError:
                    raise Exception("exception %s is not found:%s"%(msg[2], msg[3]))
            else:
                raise Exception("unknow message type")
        else:
            #if timeout we have to close the old one and create a new one
            #TODO:(cd)according to the zmq's doc, it's a bad behaviour to create and close lots of sockets
            self.__onTimeout()
            raise Exception("timeout")

    def __close(self):
        self.__poller.unregister(self.__socket)
        self.__socket.setsockopt(zmq.LINGER, 0)
        self.__socket.close()
        self.__context.term()

    def __getattr__(self, name):
        return Call(name, self)

class AsyncClient():
    """
    not threaded safe
    """
    def __init__(self, address, timeout=60):
        self.__exitFlag = threading.Event()
        self.__address = address
        self.__timeout = timeout
        self.__uid = uuid.uuid1().hex
        self.__context = zmq.Context(1)
        self.__pushSocket = self.__context.socket(zmq.PUSH)
        self.__ioThread = threading.Thread(target=self.__io)
        self.__ioThread.start()
        self.__pushSocket.connect("inproc://zmqdrpc-%s"%self.__uid)
        self.__replays = {}

    def __onCall(self, name, args, kwargs):
        #TODO:(cd)make request id short
        requestId = uuid.uuid1().hex
        msg = msgpack.packb(["request", requestId, name, args, kwargs])
        replay = Replay(requestId, time.time()+self.__timeout)
        self.__replays[requestId] = replay
        self.__pushSocket.send_multipart([msg])
        return replay

    def __io(self):
        self.__pullSocket = self.__context.socket(zmq.PULL)
        self.__pullSocket.bind("inproc://zmqdrpc-%s"%self.__uid)
        self.__socket = self.__context.socket(zmq.DEALER)
        self.__socket.connect("tcp://%s:%s"%self.__address)
        self.__poller = zmq.Poller()
        self.__poller.register(self.__pullSocket, zmq.POLLIN)
        self.__poller.register(self.__socket, zmq.POLLIN)
        while 1:
            socks = dict(self.__poller.poll(1000))
            if socks.get(self.__pullSocket) == zmq.POLLIN:
                frames = self.__pullSocket.recv_multipart()
                #add an empty frame to behave like REQ
                self.__socket.send_multipart(['']+frames)
            if socks.get(self.__socket) == zmq.POLLIN:
                #skip the empty frame
                frames = self.__socket.recv_multipart()[1:]
                if len(frames) == 1:
                    msg = msgpack.unpackb(frames[0])
                else:
                    LOGGER.warn("recv unknown format message")
                    continue
                if msg[0] == "replay":
                    rep = msg[2]
                    isError = False
                elif msg[0] == "Exception":
                    try:
                        rep = getattr(exceptions, msg[2])(msg[3])
                        isError = True
                    except AttributeError:
                        rep = Exception("exception %s is not found:%s"%(msg[2], msg[3]))
                        isError = True
                else:
                    LOGGER.warn("unknow message type: %s"%msg[0])
                    continue
                requestId = msg[1]
                if requestId in self.__replays:
                    self.__replays[requestId]._Replay__set(rep, isError)
                    self.__replays.pop(requestId)
            now = time.time()
            for key, value in self.__replays.items():
                if now > value.timeoutAt:
                    self.__replays.pop(key)
            if self.__exitFlag.isSet():
                break

    def __getattr__(self, name):
        return Call(name, self)
