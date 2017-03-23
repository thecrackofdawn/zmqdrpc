# Distributed RPC based on zeromq
---
## why another rpc based on zmq
I search a lot rpc projects and zerorpc seems to be a promising one.I have a web service based on flask, but zerorpc uses gevent heavily.The problem arised when i mixed flask(serve with classic process and thread) with gevent.I don't want to serve with gevent, so i can't use zerorpc :(.I think maybe it is the right time for me to learn zeromq after hearing it a lot.Then i start writing this rpc based on classic process and thread.
## features
* implemented by classic process and thread(no gevent involved)
* gevent compatible
* support sync and async mode
* make client and worker threaded safe(zeromq itself is not threaded safe)
* **distributed**. You can have more then one worker on the remote side and the worker can run on different machines
* support both python2 and python3

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
        heartbeat=1, liveness=5)
    broker.serve_forever()
```    
we define a broker to accept requests from clients on port 5555 and dispatch those requests to workers connected to port 5556. The broker will check workers every 1(`heartbeat`) second and discard workers who don't respond in 5(`liveness`) seconds. The `heartbeat`should be equal to the one define in worker.  
start the broker `python broker.py`
### worker
```python
from zmqdrpc import Worker

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

    #or you can register by instance
    #worker.register_instance(methods())

    worker.serve_forever()
```     
we define a worker connected to the broker's backend. the worker will ping the broker every 1(`heartbeat`) second and spawn two threads to process requests.   
start the worker `python worker.py`

### client
```python
from zmqdrpc import Client

client = Client(('127.0.0.1', 5555), timeout=20, threaded=True)
def run():
    print "call add_one(1)", client.add_one(1)
    print "call add_two(2)", client.add_two(2)
    print "call divide(1, 0)", client.divide(1, 0)

if __name__ == "__main__":
    run()
```
The client connects to the broker's front end and calls remote methods. An exception will be raised if the remote methods not return after 20(`timeout`) seconds. If `threaded` is True, you can share the client over all threads in a process.

By running `python client.py`, you will get    

```
call add_one(1) 2
call add_two(2) 4
call divide(1, 0)
Traceback (most recent call last):
  File "client.py", line 10, in <module>
    run()
  File "client.py", line 7, in run
    print "call divide(1, 0)", client.divide(1, 0)
  File "/Volumes/personal/personalwork/project/zmqdrpc/examples/zmqdrpc/client.py", line 52, in __call__
    return getattr(self.client, ''.join(['_', self.client.__class__.__name__, "__on_call"]))(self.name, args, kwargs)
  File "/Volumes/personal/personalwork/project/zmqdrpc/examples/zmqdrpc/client.py", line 111, in __on_call
    raise RemoteException("{0}".format(msg[2]))
zmqdrpc.exceptions.RemoteException: ZeroDivisionError('integer division or modulo by zero',)
```    
we see ZeroDivisionError is Raised when calling divide(1, 0). If you set `exception_verbose=True` when initialize worker, you will get the traceback for the exception.

```
call add_one(1) 2
call add_two(2) 4
call divide(1, 0)
Traceback (most recent call last):
  File "client.py", line 10, in <module>
    run()
  File "client.py", line 7, in run
    print "call divide(1, 0)", client.divide(1, 0)
  File "/Volumes/personal/personalwork/project/zmqdrpc/examples/zmqdrpc/client.py", line 52, in __call__
    return getattr(self.client, ''.join(['_', self.client.__class__.__name__, "__on_call"]))(self.name, args, kwargs)
  File "/Volumes/personal/personalwork/project/zmqdrpc/examples/zmqdrpc/client.py", line 111, in __on_call
    raise RemoteException("{0}".format(msg[2]))
zmqdrpc.exceptions.RemoteException: Traceback (most recent call last):
  File "/Volumes/personal/personalwork/project/zmqdrpc/examples/zmqdrpc/worker.py", line 113, in dispatch_call
    result = func(*args, **kwargs)
  File "worker.py", line 10, in divide
    return a/b
ZeroDivisionError: integer division or modulo by zero
```   
### together with gevent
zmqdrpc can be used together with gevent. There is a function named `gevent_patch` in `zmqdrpc.broker`,`zmqdrpc.worker`and`zmqdrpc.client`module. Call it before you initialize any instance. For example, if you want to run Worker under gevent, you can import like this:

```python
from gevent.monkey import patch_all
from zmqdrpc.worker import gevent_patch, Worker

gevent_patch()
patch_all()
```

Have fun!!!
