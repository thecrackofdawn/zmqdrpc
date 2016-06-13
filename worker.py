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
        self.threadNum = thread if thread>0 else 1
        self.threads = []
        if identity is None:
            self.identity = uuid.uuid1().hex
        else:
            self.identity = identity
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, self.identity)
        self.socket.connect("tcp://%s:%s"%address)
        #zmqsocket is not threadsafe
        self.workerSocket = self.context.socket(zmq.ROUTER)
        self.workerSocket.bind("inproc://workers")
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.poller.register(self.workerSocket, zmq.POLLIN)
        self.tindex = 0


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
                    #TODO:(cd)how does async client to deal this backMsg?
                    backMsg = msgpack.packb(["Exception", '', "Exception", "unknown message format"])
                    socket.send_multipart([address, '', backMsg])
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
                    backMsg = msgpack.packb(["Exception", requestId, err.__class__.__name__, err.message])
                    socket.send_multipart([address, '', backMsg])
                    continue
                backMsg = msgpack.packb(["replay", requestId, result])
                socket.send_multipart([address, '', backMsg])
            if self.exitFlag.isSet():
                #current used for unittest
                break
        poller.unregister(socket)
        socket.close()

    def heartbeat(self):
        if time.time() >= self.heartbeatAt:
            msg = msgpack.packb(["ping"])
            self.socket.send_multipart([msg])
            self.heartbeatAt = time.time() + self.heartbeatInterval

    def serve_forever(self):
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
                #current used for unittest
                for t in self.threads:
                    t.join()
                break
        self.socket.close()
        self.workerSocket.close()
        self.context.term()
