import unittest
import worker, broker, client
import threading
import time

class StopableThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

class BasicUsage(unittest.TestCase):
    def setUp(self):
        self.clean = []

    #@unittest.skip("")
    def test_one2w1t1(self):
        def echo(msg):
            return msg
        self.worker = worker.Worker(("127.0.0.1", 5556), "id1")
        self.clean.append(self.worker)
        self.worker.daemon = True
        self.broker = broker.Broker()
        self.clean.append(self.broker)
        self.broker.daemon = True
        client_ = client.Client(("127.0.0.1", 5555), timeout=5)
        self.worker.register_function(echo, "echo")
        workerThread = threading.Thread(target=self.worker.serve_forever)
        workerThread.start()
        brokerThread = threading.Thread(target=self.broker.serve_forever)
        brokerThread.start()
        sendMsg = "hiii"
        msg = client_.echo(sendMsg)
        self.assertEqual(msg, sendMsg)
        #kwargs
        msg = client_.echo(msg=sendMsg)
        self.assertEqual(msg, sendMsg)
        while self.clean:
            self.clean.pop().exitFlag.set()
        time.sleep(2)

    def test_two2w1t2(self):
        def curThread():
            time.sleep(5)
            tid = threading.current_thread().ident
            return tid
        self.worker = worker.Worker(("127.0.0.1", 5556), "id1", 2)
        self.clean.append(self.worker)
        self.worker.daemon = True
        self.broker = broker.Broker()
        self.clean.append(self.broker)
        self.broker.daemon = True
        client_1 = client.Client(("127.0.0.1", 5555), timeout=20)
        client_2 = client.Client(("127.0.0.1", 5555), timeout=20)
        self.worker.register_function(curThread, "curThread")
        workerThread = threading.Thread(target=self.worker.serve_forever)
        workerThread.start()
        brokerThread = threading.Thread(target=self.broker.serve_forever)
        brokerThread.start()
        def anony(inst, attr, client):
            setattr(inst, attr, client.curThread())
        t1 = threading.Thread(target=anony, args=(self, "msg1", client_1))
        t2 = threading.Thread(target=anony, args=(self, "msg2", client_2))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertNotEqual(self.msg1, self.msg2)
        while self.clean:
            self.clean.pop().exitFlag.set()
        time.sleep(2)

    @unittest.skip("")
    def test_one2w2t1(self):
        pass

    def tearDown(self):
        while self.clean:
            self.clean.pop().exitFlag.set()
        time.sleep(2)

if __name__ == "__main__":
    unittest.main()
