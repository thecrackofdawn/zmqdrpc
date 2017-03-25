
import importlib
import logging
import threading
import time
import traceback
import uuid

import zmq
import msgpack


LOGGER = logging.getLogger("zmqdrpc-worker")
LOGGER.setLevel("INFO")
_ = logging.StreamHandler()
_.setLevel('INFO')
_.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
LOGGER.addHandler(_)

def gevent_patch():
    global zmq
    zmq = importlib.import_module('zmq.green')

class RegisterFunctions():
    def __init__(self):
        self.registered_names = []

    def add_func(self, func, name):
        self.registered_names.append(name)
        setattr(self, name, func)

class Worker():
    def __init__(self, address, identity=None, thread=1, heartbeat=1, exception_verbose=False):
        self.exit_flag = threading.Event()
        self.address = address
        self.heartbeat_at = time.time()
        self.heartbeat = heartbeat
        self.exception_verbose = exception_verbose
        self.context = zmq.Context(1)
        self.rpc_instance = RegisterFunctions()
        self.thread_num = int(thread) if thread > 0 else 1
        self.threads = []
        if identity is None:
            self.identity = uuid.uuid1().hex.encode("utf-8")
        else:
            self.identity = identity.encode("utf-8")
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, self.identity)
        self.socket.connect("tcp://%s:%s"%address)
        #zmqsocket is not threadedsafe
        self.worker_socket = self.context.socket(zmq.ROUTER)
        self.worker_socket.bind("inproc://workers")
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.poller.register(self.worker_socket, zmq.POLLIN)
        self.tindex = 0

    def register_function(self, function, name):
        if not isinstance(self.rpc_instance, RegisterFunctions):
            raise Exception("you have already registered an instance")
        if not callable(function):
            raise Exception("%s is not callable"%name)
        self.rpc_instance.add_func(function, name)

    def register_instance(self, instance):
        if isinstance(self.rpc_instance, RegisterFunctions):
            if self.rpc_instance.registered_names:
                raise Exception("you have already registered function")
        else:
            raise Exception("you have already registered an instance")
        self.rpc_instance = instance

    def check_msg(self, msg):
        #msg:[action, actionId, funcname, args, kwargs]
        if not len(msg) == 5:
            return False
        else:
            return True

    def check_threads(self):
        self.threads = [t for t in self.threads if t.is_alive()]
        shortage = self.thread_num - len(self.threads)
        for _ in range(shortage if shortage > 0 else 0):
            t = threading.Thread(target=self.dispatch_call)
            self.threads.append(t)
            t.start()

    def get_thread(self):
        index = self.tindex%len(self.threads)
        self.tindex = (self.tindex+1)%len(self.threads)
        return str(self.threads[index].ident).encode("ascii")

    def dispatch_call(self):
        socket = self.context.socket(zmq.DEALER)
        socket.setsockopt(zmq.IDENTITY, str(threading.current_thread().ident).encode("ascii"))
        socket.connect("inproc://workers")
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        try:
            while 1:
                socks = dict(poller.poll(1000))
                if socks.get(socket) == zmq.POLLIN:
                    address, msg = socket.recv_multipart()
                    msg = msgpack.unpackb(msg, encoding="utf-8")
                    if not self.check_msg(msg):
                        #this should not happen except internal error of zmqdrpc
                        LOGGER.warning("receive unknown message from %s"%address)
                        continue
                    msg_len = len(msg)
                    request_id = msg[1]
                    func_name = msg[2]
                    args = msg[3] if msg_len >= 4 else None
                    kwargs = msg[4] if msg_len == 5 else None
                    try:
                        func = getattr(self.rpc_instance, func_name)
                        result = func(*args, **kwargs)
                    except Exception as err:
                        if self.exception_verbose:
                            err_msg = traceback.format_exc()
                        else:
                            #encode for python3
                            err_msg = repr(err).encode('utf-8')
                        back_msg = msgpack.packb([b"exception", request_id, err_msg], encoding="utf-8")
                        socket.send_multipart([b'exception', address, b'', back_msg])
                        continue
                    back_msg = msgpack.packb([b"replay", request_id, result], encoding="utf-8")
                    socket.send_multipart([b'replay', address, b'', back_msg])
                if self.exit_flag.isSet():
                    break
        except:
            LOGGER.error(traceback.format_exc())
        finally:
            poller.unregister(socket)
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()

    def heartbeating(self):
        if time.time() >= self.heartbeat_at:
            self.socket.send_multipart([b'ping'])
            self.heartbeat_at = time.time() + self.heartbeat

    def serve_forever(self):
        LOGGER.info("register worker %s", self.identity)
        try:
            while 1:
                self.check_threads()
                self.heartbeating()
                socks = dict(self.poller.poll(self.heartbeat*1000))
                if socks.get(self.socket) == zmq.POLLIN:
                    frames = self.socket.recv_multipart()
                    if len(frames) == 3:
                        address, _, msg = frames
                        self.worker_socket.send_multipart([self.get_thread(), address, msg])
                if socks.get(self.worker_socket) == zmq.POLLIN:
                    frames = self.worker_socket.recv_multipart()[1:]
                    self.socket.send_multipart(frames)
                if self.exit_flag.isSet():
                    for t in self.threads:
                        t.join()
                    break
        except KeyboardInterrupt:
            LOGGER.info("interrupt reveived, stopping...")
        finally:
            self.exit_flag.set()
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.worker_socket.setsockopt(zmq.LINGER, 0)
            self.worker_socket.close()
            #wait dispatch_call to exit
            time.sleep(2)
            self.context.term()
