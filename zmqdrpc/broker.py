
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

class Balancer(object):
    def __init__(self, heartbeat, liveness):
        self.heartbeat = heartbeat
        self.liveness = liveness

    def get(self):
        raise NotImplementedError()

    def update(self, worker, action):
        raise NotImplementedError()

    def check(self):
        raise NotImplementedError()

    def is_empty(self):
        raise NotImplementedError()

    def status(self):
        pass

class RoundRobin(Balancer):
    def __init__(self, heartbeat, liveness):
        self.pool = defaultdict(dict)
        self.index = 0
        self.liveness = liveness
        self.heartbeat = heartbeat
        self.check_at = time.time()
        super(RoundRobin, self).__init__(heartbeat, liveness)

    def get(self):
        workers = list(self.pool.keys())
        if self.index < len(workers):
            worker = workers[self.index]
            self.index = (self.index+1)%len(workers)
        else:
            raise Exception("no worker available")
        return worker

    def update(self, worker, action):
        is_new = not worker in self.pool
        if action == b"ping":
            if is_new:
                LOGGER.info("register worker %s", worker)
            self.pool[worker]["missing_count"] = 0
        else:
            if not is_new:
                self.pool[worker]["missing_count"] = 0

    def check(self):
        if not (time.time() >= self.check_at):
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
        self.check_at = time.time() + self.heartbeat

    def is_empty(self):
        if len(self.pool):
            return False
        else:
            return True

    def status(self):
        status = {}
        status['workers'] = self.pool
        status['liveness'] = self.liveness
        status['heartbeat'] = self.heartbeat
        return status

class IdleFirst(Balancer):
    def __init__(self, heartbeat, liveness):
        super(IdleFirst, self).__init__(heartbeat, liveness)
        self.pool = {}
        self.check_at = time.time()

    def get(self):
        if not self.is_empty():
            stat, worker = min(([self.pool[k], k] for k in self.pool))
            stat[0] += 1
            stat[1] = int(time.time())
            return worker
        else:
            raise Exception("no worker available")

    def update(self, worker, action):
        if action == b'ping':
            if worker in self.pool:
                self.pool[worker][2] = 0
            else:
                LOGGER.info("register worker %s", worker)
                self.pool[worker] = [0, 0, 0]#pendingTasks, lastUsedAt, missingHeartbeat
        elif (action == b'replay' or action == b'exception') and worker in self.pool:
            #TODO:(cd)once worker reconnect after timeout, the response it holds back will make the pendingTasks minus
            #lets fix it in a Q&D way for now by not allowed pendingTasks to be minus
            if self.pool[worker][0] > 0:
                self.pool[worker][0] -= 1

    def check(self):
        if not (time.time() >= self.check_at):
            return
        def update(items):
            for item in items:
                if not item[1][2] > self.liveness:
                    item[1][2] += 1
                    yield item
                else:
                    LOGGER.info("timeout for worker %s, wipe it", item[0])

        self.pool = dict(update(self.pool.items()))
        self.check_at += self.heartbeat

    def is_empty(self):
        return not bool(self.pool)

    def status(self):
        status = {}
        status['workers'] = self.pool
        status['liveness'] = self.liveness
        status['heartbeat'] = self.heartbeat
        return status

class Broker():
    def __init__(self, frontend=("127.0.0.1", 5555), backend=("127.0.0.1", 5556),
        heartbeat=1, liveness=5, balancer=RoundRobin, manager=("127.0.0.1", 5557)):
        self.exit_flag = threading.Event()
        self.context = zmq.Context(1)
        self.frontend = self.context.socket(zmq.ROUTER)
        self.backend = self.context.socket(zmq.ROUTER)
        self.manager = self.context.socket(zmq.REP)
        self.frontend.bind("tcp://%s:%s"%frontend)
        self.backend.bind("tcp://%s:%s"%backend)
        self.manager.bind("tcp://%s:%s"%manager)

        self.apoller = zmq.Poller()
        self.bpoller = zmq.Poller()
        self.apoller.register(self.frontend, zmq.POLLIN)
        self.apoller.register(self.backend, zmq.POLLIN)
        self.apoller.register(self.manager, zmq.POLLIN)
        self.bpoller.register(self.manager, zmq.POLLIN)
        self.bpoller.register(self.backend, zmq.POLLIN)
        self.heartbeat = heartbeat
        self.balancer = balancer(heartbeat, liveness)

    def serve_forever(self):
        LOGGER.info("start serving...")
        try:
            while 1:
                self.balancer.check()
                #wait until there are works available
                if self.balancer.is_empty():
                    socks = dict(self.bpoller.poll())
                else:
                    socks = dict(self.apoller.poll(self.heartbeat*1000))
                if socks.get(self.backend) == zmq.POLLIN:
                    frames = self.backend.recv_multipart()
                    #[worker, action]
                    if len(frames) == 2:
                        worker, action = frames
                        self.balancer.update(worker, action)
                    #[worker, aciton, client, '', msg]
                    elif len(frames) == 5:
                        self.balancer.update(frames[0], frames[1])
                        msg = frames[2:]
                        self.frontend.send_multipart(msg)
                if socks.get(self.frontend) == zmq.POLLIN:
                    msg = self.frontend.recv_multipart()
                    worker = self.balancer.get()
                    self.backend.send_multipart([worker] + msg)
                if socks.get(self.manager) == zmq.POLLIN:
                    #[action]
                    msg = self.manager.recv_multipart()
                    if msg[0] == b"status":
                        rep = msgpack.packb(self.balancer.status(), encoding="utf-8")
                        self.manager.send_multipart([rep])
                if self.exit_flag.isSet():
                    break
        finally:
            self.frontend.setsockopt(zmq.LINGER, 0)
            self.backend.setsockopt(zmq.LINGER, 0)
            self.manager.setsockopt(zmq.LINGER, 0)
            self.frontend.close()
            self.backend.close()
            self.manager.close()
            self.context.term()
