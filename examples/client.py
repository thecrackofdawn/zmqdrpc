from zmqdrpc import Client

client = Client(('127.0.0.1', 5555), timeout=20, threaded=True)
def run():
    print "call add_one(1)", client.add_one(1)
    print "call add_two(2)", client.add_two(2)
    print "call divide(1, 0)", client.divide(1, 0)

if __name__ == "__main__":
    run()
