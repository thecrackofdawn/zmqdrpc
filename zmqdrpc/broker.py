
from collections import defaultdict
import importlib
import logging
import time
import threading

import msgpack
import zmq

LOGGER = logging.getLogger("zmqdrpc-broker")
LOGGER.setLevel("INFO")
_ = logging.StreamHandler()
_.setLevel('INFO')
_.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
LOGGER.addHandler(_)

def gevent_patch():
    global zmq
    zmq = importlib.import_module('zmq.green')

class WorkerQueue():
    def __init__(self, interval, liveness):
        self.pool = defaultdict(dict)
        self.index = 0
        self.liveness = liveness
        self.interval = interval
        self.check_at = time.time()

    def get(self):
        workers = list(self.pool.keys())
        if self.index < len(workers):
            worker = workers[self.index]
            self.index = (self.index+1)%len(workers)
        else:
            worker = None
        return worker

    def update(self, worker, is_ping=False):
        is_new = not worker in self.pool
        if is_ping:
            if is_new:
                LOGGER.info("register worker %s", worker)
            self.pool[worker]["missing_count"] = 0
        else:
            if not is_new:
                self.pool[worker]["missing_count"] = 0

    def check(self):
        if not (time.time() - self.check_at >= self.interval):
            return
        del_list = []
        for work, state in self.pool.items():
            if state["missing_count"] > self.liveness:
                del_list.append(work)
            else:
                state["missing_count"] += 1
        for worker in del_list:
            try:
                index = list(self.pool.keys()).index(worker)
            except ValueError:
                continue
            if index > self.index:
                self.pool.pop(worker)
            elif index == self.index:
                self.pool.pop(worker)
                if len(self.pool) == 0:
                    self.index = 0
                else:
                    self.index = (self.index)%len(self.pool)
            else:
                self.pool.pop(worker)
                self.index -= 1
            LOGGER.info("timeout for worker %s, wipe it", worker)
        self.check_at = time.time() + self.interval

    def empty(self):
        if len(self.pool):
            return False
        else:
            return True

class Broker():
    def __init__(self, frontend=("127.0.0.1", 5555), backend=("127.0.0.1", 5556),
        heartbeat=1, liveness=5):
        self.exit_flag = threading.Event()
        self.context = zmq.Context(1)
        self.frontend = self.context.socket(zmq.ROUTER)
        self.backend = self.context.socket(zmq.ROUTER)
        self.frontend.bind("tcp://%s:%s"%frontend)
        self.backend.bind("tcp://%s:%s"%backend)
        self.apoller = zmq.Poller()
        self.bpoller = zmq.Poller()
        self.apoller.register(self.frontend, zmq.POLLIN)
        self.apoller.register(self.backend, zmq.POLLIN)
        self.bpoller.register(self.backend, zmq.POLLIN)
        self.heartbeat = heartbeat
        self.worker_queue = WorkerQueue(heartbeat, liveness)

    def serve_forever(self):
        LOGGER.info("start serving...")
        while 1:
            if self.worker_queue.empty():
                socks = dict(self.bpoller.poll())
            else:
                socks = dict(self.apoller.poll(self.heartbeat*1000))
            if socks.get(self.backend) == zmq.POLLIN:
                frames = self.backend.recv_multipart()
                if len(frames) == 2:
                    ping_code = msgpack.unpackb(frames[1], encoding='utf-8')
                    if ping_code == "ping":
                        self.worker_queue.update(frames[0], is_ping=True)
                elif len(frames) > 2:
                    self.worker_queue.update(frames[0])
                    msg = frames[1:]
                    self.frontend.send_multipart(msg)
            self.worker_queue.check()
            if socks.get(self.frontend) == zmq.POLLIN:
                msg = self.frontend.recv_multipart()
                worker = self.worker_queue.get()
                self.backend.send_multipart([worker] + msg)
            if self.exit_flag.isSet():
                #current used for test
                break
        self.frontend.close()
        self.backend.close()
        self.context.term()
