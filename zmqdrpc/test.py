import unittest
import worker, broker, client
import threading
import time


class BasicUsageSync(unittest.TestCase):
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

    def test_timeout(self):
        def timeout(sec):
            time.sleep(sec)
        def echo(msg):
            return msg
        self.worker = worker.Worker(("127.0.0.1", 5556), thread=2)
        self.clean.append(self.worker)
        self.worker.daemon = True
        self.broker = broker.Broker()
        self.clean.append(self.broker)
        self.broker.daemon = True
        self.worker.register_function(timeout, "timeout")
        self.worker.register_function(echo, "echo")
        workerThread = threading.Thread(target=self.worker.serve_forever)
        workerThread.start()
        brokerThread = threading.Thread(target=self.broker.serve_forever)
        brokerThread.start()
        client_ = client.Client(("127.0.0.1", 5555), timeout=1)
        with self.assertRaises(Exception) as cm:
            client_.timeout(2)
        self.assertEqual(cm.exception.message, "timeout")
        with client.Timeout(client_, 5):
            self.assertEqual(client_.echo("hii"), "hii")
        time.sleep(2)

    @unittest.skip("")
    def test_one2w2t1(self):
        pass

    def tearDown(self):
        while self.clean:
            self.clean.pop().exitFlag.set()
        time.sleep(2)

class BasicUsageAsync(unittest.TestCase):
    def setUp(self):
        self.clean = []

    def test_one2w1t1(self):
        def echo(msg):
            return msg
        self.worker = worker.Worker(("127.0.0.1", 5556), "id1")
        self.clean.append(self.worker.exitFlag)
        self.worker.daemon = True
        self.broker = broker.Broker()
        self.clean.append(self.broker.exitFlag)
        self.broker.daemon = True
        client_ = client.AsyncClient(("127.0.0.1", 5555), timeout=5)
        self.clean.append(client_._AsyncClient__exitFlag)
        self.worker.register_function(echo, "echo")
        workerThread = threading.Thread(target=self.worker.serve_forever)
        workerThread.start()
        brokerThread = threading.Thread(target=self.broker.serve_forever)
        brokerThread.start()
        sendMsg = "hiii"
        msg = client_.echo(sendMsg).get()
        self.assertEqual(msg, sendMsg)
        #kwargs
        msg = client_.echo(msg=sendMsg).get()
        self.assertEqual(msg, sendMsg)

    def tearDown(self):
        while self.clean:
            self.clean.pop().set()
        time.sleep(2)

if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(BasicUsageSync("test_one2w1t1"))
    suite.addTest(BasicUsageSync("test_two2w1t2"))
    suite.addTest(BasicUsageSync("test_timeout"))
    suite.addTest(BasicUsageAsync("test_one2w1t1"))
    runner = unittest.TextTestRunner()
    runner.run(suite)
