from zmqdrpc import Client

client = Client(('127.0.0.1', 5555), timeout=20, threadedSafe=True)
def run():
    print "call addOne(1)", client.addOne(1)
    print "call addTwo(2)", client.addTwo(2)
    print "call divide(1, 0)", client.divide(1, 0)

if __name__ == "__main__":
    run()
