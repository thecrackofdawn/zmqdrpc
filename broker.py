import zmq
from collections import defaultdict
import time
import threading

class WorkerQueue():
    def __init__(self, interval, liveness):
        self.pool = defaultdict(dict)
        self.index = 0
        self.liveness = liveness
        self.interval = interval
        self.checkAt = time.time()

    def get(self):
        workers = self.pool.keys()
        if self.index < len(workers):
            worker = workers[self.index]
            self.index = (self.index+1)%len(workers)
        else:
            worker = None
        return worker

    def update(self, work):
        self.pool[work]["missingCount"] = 0

    def check(self):
        if not (time.time() - self.checkAt >= self.interval):
            return
        delList = []
        for work, state in self.pool.viewitems():
            if state["missingCount"] > self.liveness:
                delList.append(work)
            else:
                state["missingCount"] += 1
        for worker in delList:
            try:
                index = self.pool.keys().index(worker)
            except ValueError:
                continue
            if index > self.index:
                self.pool.pop(worker)
            elif index == self.index:
                self.pool.pop(worker)
                if len(self.pool) == 0:
                    self.index = 0
                else:
                    self.index = (self.index+1)%len(self.pool)
            else:
                self.pool.pop(worker)
                self.index -= 1
        self.checkAt = time.time() + self.interval

    def empty(self):
        if len(self.pool):
            return False
        else:
            return True

class Broker():
    def __init__(self, frontend=("127.0.0.1", 5555), backend=("127.0.0.1", 5556),
        heartbeatInterval=1, heartbeatLiveness=5):
        self.exitFlag = threading.Event()
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
        self.heartbeatInterval = heartbeatInterval
        self.workerQueue = WorkerQueue(heartbeatInterval, heartbeatLiveness)

    def serve_forever(self):
        while 1:
            if self.workerQueue.empty():
                socks = dict(self.bpoller.poll())
            else:
                socks = dict(self.apoller.poll(self.heartbeatInterval*1000))
            if socks.get(self.backend) == zmq.POLLIN:
                frames = self.backend.recv_multipart()
                self.workerQueue.update(frames[0])
                if len(frames) > 2:
                    msg = frames[1:]
                    self.frontend.send_multipart(msg)
            self.workerQueue.check()
            if socks.get(self.frontend) == zmq.POLLIN:
                msg = self.frontend.recv_multipart()
                worker = self.workerQueue.get()
                self.backend.send_multipart([worker] + msg)
            if self.exitFlag.isSet():
                #current used for test
                break
        self.frontend.close()
        self.backend.close()
        self.context.term()
