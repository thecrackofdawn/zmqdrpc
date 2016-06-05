import zmq
import msgpack
import uuid
import threading
import time

class RegisterFunctions():
    def __init__(self):
        self.registered_names = []

    def add_func(self, func, name):
        self.registered_names.append(name)
        setattr(self, name, func)

class Worker():
    def __init__(self, address, identity=None, thread=1, heartbeatInterval=1):
        self.exitFlag = threading.Event()
        self.address = address
        self.heartbeatAt = time.time()
        self.heartbeatInterval = heartbeatInterval
        self.context = zmq.Context(1)
        self.rpcInstance = RegisterFunctions()
        self.threadNum = thread
        self.threads = []
        if identity is None:
            self.identity = uuid.uuid1().hex
        else:
            self.identity = identity
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, self.identity)
        self.socket.connect("tcp://%s:%s"%address)
        #zmqsocket is not threadsafe
        self.workerSocket = self.context.socket(zmq.REP)
        self.workerSocket.bind("inproc://workers")
        self.poller = zmq.Poller()
        self.innerPoller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.poller.register(self.workerSocket, zmq.POLLIN)
        self.innerPoller.register(self.workerSocket, zmq.POLLIN)

    def register_function(self, function, name):
        if not isinstance(self.rpcInstance, RegisterFunctions):
            raise Exception("you have already registered an instance")
        if not callable(function):
            raise Exception("%s is not callable"%name)
        self.rpcInstance.add_func(function, name)

    def register_instance(self, instance):
        if isinstance(self.rpcInstance, RegisterFunctions) and self.rpcInstance.register_instance:
            raise Exception("you have already registered functions")
        self.rpcInstance = instance

    def check_msg(self, msg):
        #msg:[action, funcname, args, kwargs]
        if not len(msg) == 4:
            return False
        else:
            return True

    def dispatch_call(self, address, name, args, kwargs):
        socket = self.context.socket(zmq.REQ)
        socket.connect("inproc://workers")
        try:
            func = getattr(self.rpcInstance, name)
            result = func(*args, **kwargs)
        except Exception, err:
            msg = msgpack.packb(["Exception", type(err), err.message])
            socket.send_multipart([address, '', msg])
        msg = msgpack.packb(["replay", result])
        socket.send_multipart([address, '', msg])
        response = ""
        try:
            response = socket.recv()
        except zmq.ContextTerminated:
            socket.close()
        else:
            if  response == "done":
                socket.close()
            else:
                #TODO:(cd)warn here
                socket.close()

    def heartbeat(self):
        if time.time() >= self.heartbeatAt:
            msg = msgpack.packb(["ping"])
            self.socket.send_multipart([msg])
            self.heartbeatAt = time.time() + self.heartbeatInterval

    def serve_forever(self):
        while 1:
            socks = {}
            threadNum = len(self.threads)
            self.threads = filter(lambda t: t.is_alive(), self.threads)
            self.heartbeat()
            if threadNum < self.threadNum:
                socks = dict(self.poller.poll(self.heartbeatInterval*1000))
            else:
                socks = dict(self.innerPoller.poll(self.heartbeatInterval*1000))
            if socks.get(self.socket) == zmq.POLLIN:
                frames = self.socket.recv_multipart()
                if len(frames) == 3:
                    address, empty, msg = frames
                    msg = msgpack.unpackb(msg)
                    if not self.check_msg(msg):
                        backMsg = msgpack.packb(["Exception", "Exception", "unknown msg format"])
                        self.socket.send_multipart([address, '', backMsg])
                        continue
                    msgLen = len(msg)
                    funcName = msg[1]
                    args = msg[2] if msgLen >=3 else None
                    kwargs = msg[3] if msgLen == 4 else None
                    t = threading.Thread(target=self.dispatch_call, args=(address, funcName, args, kwargs))
                    self.threads.append(t)
                    t.start()
            if socks.get(self.workerSocket) == zmq.POLLIN:
                frames = self.workerSocket.recv_multipart()
                self.socket.send_multipart(frames)
                self.workerSocket.send("done")
            if self.exitFlag.isSet():
                #current used for unittest
                break
        self.socket.close()
        self.workerSocket.close()
        self.context.term()
