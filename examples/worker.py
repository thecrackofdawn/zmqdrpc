from zmqdrpc import Worker

def addOne(num):
    return num+1

def addTwo(num):
    return num+2

def divide(a, b):
    return a/b

class methods:
    def addOne(self, num):
        return num+1

    def addTwo(self, num):
        return num+2

    def divide(self, a, b):
        return a/b

if __name__ == "__main__":
    worker = Worker(address=('127.0.0.1', 5556), thread=2, heartbeatInterval=1,
        exceptionVerbose=True)
    worker.register_function(addOne, 'addOne')
    worker.register_function(addTwo, 'addTwo')
    worker.register_function(divide, 'divide')

    #or you can register by instance
    #worker.register_instance(methods())

    worker.serve_forever()
