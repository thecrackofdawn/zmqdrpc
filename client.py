import zmq
import msgpack
import exceptions

class call():
    def __init__(self, address, context, name, timeout):
        self.name = name
        self.context = context
        self.address = address
        self.timeout = timeout*1000

    def __call__(self, *args, **kwargs):
        socket = self.context.socket(zmq.REQ)
        socket.connect("tcp://%s:%s"%self.address)
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        msg = msgpack.packb(["request", self.name, args, kwargs])
        socket.send_multipart([msg])
        timeout = False
        socks = poller.poll(self.timeout)
        if socks:
            frames = socket.recv_multipart()
            if len(frames) == 1:
                msg = msgpack.unpackb(frames[0])
            else:
                raise Exception("unknow format")
            if msg[0] == "replay":
                return msg[1]
            elif msg[0] == "Exception":
                try:
                    raise getattr(exceptions, msg[1])(msg[2])
                except AttributeError:
                    raise Exception("exception %s is not found:%s"%(msg[1], msg[2]))
            else:
                raise Exception("Unknow format")
        else:
            timeout =  True
        poller.unregister(socket)
        socket.setsockopt(zmq.LINGER, 0)
        socket.close()
        if timeout:
            raise Exception("timeout")


class Client():
    def __init__(self, address, timeout=60):
        self.context = zmq.Context(1)
        self.timeout = timeout
        self.address = address

    def __getattr__(self, name):
        return call(self.address, self.context, name, self.timeout)
