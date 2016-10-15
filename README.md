# Distributed RPC based on zeromq
---
## why another rpc based on zmq
I search a lot rpc projects and zerorpc seems to be a promising one.I have a web service based on flask, but zerorpc uses gevent heavily.The problem arised when i mixed flask(serve with classic process and thread) with gevent.I don't want to serve with gevent, so i can't use zerorpc :(.I think maybe it is the right time for me to learn zeromq after hearing it a lot.Then i start writing this rpc based on classic process and thread.
## features
* implemented by classic process and thread(no gevent involved)
* support sync and async mode
* make client and worker threaded safe(zeromq itself is not threaded safe)
* **distributed**. You can have more then one worker on the remote side and the worker can run on different machines.

## warning
Though i use it in my project, i don't think it is production ready. This project is not fully tested and the performance is unknown. You may help to improve this.

## archicture
**The Paranoid Pirate Pattern**(without Retry layer in the client side)
![archicture](https://raw.githubusercontent.com/booksbyus/zguide/master/images/fig49.png)   
(name and picture borrowed from [zeromq](http://zguide.zeromq.org/py:all#Robust-Reliable-Queuing-Paranoid-Pirate-Pattern))

## install
```sh
git clone https://github.com/thecrackofdawn/zmqdrpc.git
cd zmqdrpc
python setup.py install
```
## usage
To make zmqdrpc work, we must define a broker, worker and client.
### broker
broker.py    

```python
from zmqdrpc import Broker

if __name__ == "__main__":
    broker = Broker(frontend=('127.0.0.1', 5555), backend=('127.0.0.1', 5556),
    	heartbeatInterval=1, heartbeatLiveness=5)
    broker.serve_forever()

```    
we define a broker to accept requests from clients on port 5555 and dispatch those requests to workers connected to port 5556. The broker will check workers every 1(heartbeatInterval) second and discard workers who don't respond in 5(heartbeatLiveness) seconds. The `heartbeatInterval`should be equal to the one define in worker.  
start the broker `python broker.py`
### worker
```python
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
    	exceptionVerbose=False)
    worker.register_function(addOne, 'addOne')
    worker.register_function(addTwo, 'addTwo')
    worker.register_function(divide, 'divide')

    #or you can register by instance
    #worker.register_instance(methods())

    worker.serve_forever()
```     
we define a worker connected to the broker's backend. the worker will ping the broker every 1(heartbeatInterval) second and spawn two threads to deal requests.   
start the worker `python worker.py`

### client
```python
from zmqdrpc import Client

client = Client(('127.0.0.1', 5555), timeout=20, threadedSafe=True)
def run():
    print "call addOne(1)", client.addOne(1)
    print "call addTwo(2)", client.addTwo(2)
    print "call divide(1, 0)", client.divide(1, 0)

if __name__ == "__main__":
    run()
```
The client connects to the broker's front end and calls remote methods. An exception will be raised if the remote methods not return after 20(`timeout`) seconds. If threadedSafe is True, you can share the client over all threads in a process.

By running `python client.py`, you will get    

```
call addOne(1) 2
call addTwo(2) 4
call divide(1, 0)
Traceback (most recent call last):
  File "client.py", line 10, in <module>
    run()
  File "client.py", line 7, in run
    print "call divide(1, 0)", client.divide(1, 0)
  File "/Volumes/personal/personalwork/project/zmqdrpc/examples/zmqdrpc/client.py", line 51, in __call__
    return getattr(self.client, ''.join(['_', self.client.__class__.__name__, "__onCall"]))(self.name, args, kwargs)
  File "/project/zmqdrpc/examples/zmqdrpc/client.py", line 118, in __onCall
    raise RemoteException("{0}--{1}".format(msg[2], msg[3]))
zmqdrpc.exceptions.RemoteException: ZeroDivisionError--integer division or modulo by zero
```    
we see ZeroDivisionError is Raised when calling divide(1, 0). If you set `exceptionVerbose=True` when initialize worker, you will get the traceback for the exception.

```
call divide(1, 0)
Traceback (most recent call last):
  File "client.py", line 10, in <module>
    run()
  File "client.py", line 7, in run
    print "call divide(1, 0)", client.divide(1, 0)
  File "/Volumes/personal/personalwork/project/zmqdrpc/examples/zmqdrpc/client.py", line 51, in __call__
    return getattr(self.client, ''.join(['_', self.client.__class__.__name__, "__onCall"]))(self.name, args, kwargs)
  File "/Volumes/personal/personalwork/project/zmqdrpc/examples/zmqdrpc/client.py", line 118, in __onCall
    raise RemoteException("{0}--{1}".format(msg[2], msg[3]))
zmqdrpc.exceptions.RemoteException: ZeroDivisionError--Traceback (most recent call last):
  File "/project/zmqdrpc/examples/zmqdrpc/worker.py", line 109, in dispatch_call
    result = func(*args, **kwargs)
  File "worker.py", line 20, in divide
    return a/b
ZeroDivisionError: integer division or modulo by zero
```    
Have fun!!!
