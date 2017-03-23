from zmqdrpc.worker import gevent_patch, Worker
gevent_patch()
import gevent.monkey
gevent.monkey.patch_all()

def add_one(num):
    return num+1

def add_two(num):
    return num+2

def divide(a, b):
    return a/b

class methods:
    def add_one(self, num):
        return num+1

    def add_two(self, num):
        return num+2

    def divide(self, a, b):
        return a/b

if __name__ == "__main__":
    worker = Worker(address=('127.0.0.1', 5556), thread=2, heartbeat=1,
        exception_verbose=False)
    worker.register_function(add_one, 'add_one')
    worker.register_function(add_two, 'add_two')
    worker.register_function(divide, 'divide')

    #or you can register by instance
    #worker.register_instance(methods())

    worker.serve_forever()
