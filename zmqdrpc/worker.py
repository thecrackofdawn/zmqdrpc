import zmq
import msgpack
import uuid
import threading
import time
import logging
import traceback
from exceptions import *

LOGGER = logging.getLogger("zmqdrpc-worker")
LOGGER.setLevel("INFO")
_ = logging.StreamHandler()
_.setLevel('INFO')
_.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
LOGGER.addHandler(_)

class RegisterFunctions():
    def __init__(self):
        self.registered_names = []

    def add_func(self, func, name):
        self.registered_names.append(name)
        setattr(self, name, func)

class Worker():
    def __init__(self, address, identity=None, thread=1, heartbeatInterval=1, exceptionVerbose=False):
        self.exitFlag = threading.Event()
        self.address = address
        self.heartbeatAt = time.time()
        self.heartbeatInterval = heartbeatInterval
        self.exceptionVerbose = exceptionVerbose
        self.context = zmq.Context(1)
        self.rpcInstance = RegisterFunctions()
        self.threadNum = thread if thread>0 else 1
        self.threads = []
        if identity is None:
            self.identity = uuid.uuid1().hex
        else:
            self.identity = identity
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, self.identity)
        self.socket.connect("tcp://%s:%s"%address)
        #zmqsocket is not threadedsafe
        self.workerSocket = self.context.socket(zmq.ROUTER)
        self.workerSocket.bind("inproc://workers")
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.poller.register(self.workerSocket, zmq.POLLIN)
        self.tindex = 0
        self.pingMsg = msgpack.packb(["ping"])


    def register_function(self, function, name):
        if not isinstance(self.rpcInstance, RegisterFunctions):
            raise Exception("you have already registered an instance")
        if not callable(function):
            raise Exception("%s is not callable"%name)
        self.rpcInstance.add_func(function, name)

    def register_instance(self, instance):
        if isinstance(self.rpcInstance, RegisterFunctions):
            if self.rpcInstance.registered_names:
                raise Exception("you have already registered function")
        else:
            raise Exception("you have already registered an instance")
        self.rpcInstance = instance

    def check_msg(self, msg):
        #msg:[action, actionId, funcname, args, kwargs]
        if not len(msg) == 5:
            return False
        else:
            return True

    def check_threads(self):
        self.threads = filter(lambda t: t.is_alive(), self.threads)
        shortage = self.threadNum - len(self.threads)
        for _ in range(shortage if shortage>0 else 0):
            t = threading.Thread(target=self.dispatch_call)
            self.threads.append(t)
            t.start()

    def get_thread(self):
        index = self.tindex%len(self.threads)
        self.tindex = (self.tindex+1)%len(self.threads)
        return str(self.threads[index].ident)

    def dispatch_call(self):
        socket = self.context.socket(zmq.DEALER)
        socket.setsockopt(zmq.IDENTITY, str(threading.current_thread().ident))
        socket.connect("inproc://workers")
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        while 1:
            socks = dict(poller.poll(1000))
            if socks.get(socket) == zmq.POLLIN:
                address, msg = socket.recv_multipart()
                msg = msgpack.unpackb(msg)
                if not self.check_msg(msg):
                    #this should not happen except internal error of zmqdrpc
                    LOGGER.warning("receive unknown message from %s"%address)
                    continue
                msgLen = len(msg)
                requestId = msg[1]
                funcName = msg[2]
                args = msg[3] if msgLen >=4 else None
                kwargs = msg[4] if msgLen == 5 else None
                try:
                    func = getattr(self.rpcInstance, funcName)
                    result = func(*args, **kwargs)
                except Exception, err:
                    if self.exceptionVerbose:
                        errMsg = traceback.format_exc()
                    else:
                        errMsg = err.message
                    backMsg = msgpack.packb(["Exception", requestId, err.__class__.__name__, errMsg])
                    socket.send_multipart([address, '', backMsg])
                    continue
                backMsg = msgpack.packb(["replay", requestId, result])
                socket.send_multipart([address, '', backMsg])
            if self.exitFlag.isSet():
                break
        poller.unregister(socket)
        socket.setsockopt(zmq.LINGER, 0)
        socket.close()

    def heartbeat(self):
        if time.time() >= self.heartbeatAt:
            self.socket.send_multipart([self.pingMsg])
            self.heartbeatAt = time.time() + self.heartbeatInterval

    def serve_forever(self):
        try:
            while 1:
                self.check_threads()
                self.heartbeat()
                socks = dict(self.poller.poll(self.heartbeatInterval*1000))
                if socks.get(self.socket) == zmq.POLLIN:
                    frames = self.socket.recv_multipart()
                    if len(frames) == 3:
                        address, empty, msg = frames
                        self.workerSocket.send_multipart([self.get_thread(), address, msg])
                if socks.get(self.workerSocket) == zmq.POLLIN:
                    frames = self.workerSocket.recv_multipart()[1:]
                    self.socket.send_multipart(frames)
                if self.exitFlag.isSet():
                    for t in self.threads:
                        t.join()
                    break
        except KeyboardInterrupt:
            LOGGER.info("interrupt reveived, stopping...")
        finally:
            self.exitFlag.set()
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.workerSocket.setsockopt(zmq.LINGER, 0)
            self.workerSocket.close()
            #wait dispatch_call to exit
            time.sleep(2)
            self.context.term()
