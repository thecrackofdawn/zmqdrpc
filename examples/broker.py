from zmqdrpc import Broker

if __name__ == "__main__":
    broker = Broker(frontend=('127.0.0.1', 5555), backend=('127.0.0.1', 5556),
        heartbeat=1, liveness=5)
    broker.serve_forever()
