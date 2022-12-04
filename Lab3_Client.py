import pickle


class RPCProxy:
    def __init__(self, connection):
        self._connection = connection
    def __getattr__(self, name):
        def do_rpc(*args, **kwargs):
            self._connection.send(pickle.dumps((name, args, kwargs)))
            result = pickle.loads(self._connection.recv())
            if isinstance(result, Exception):
                raise result
            return result
        return do_rpc


from multiprocessing.connection import Client
c = Client(('localhost', 17000), authkey=b'peekaboo')
proxy = RPCProxy(c)
#request an integer for basic Paxos then send RPC
runtests = input("Press enter to start Scenario 1a.")
proxy.initializeValues(200,300)
print(proxy.scenario1())
runtests = input("Press enter to start Scenario 2a.")
proxy.initializeValues(200,300)
print(proxy.scenario2())
runtests = input("Press enter to start Scenario 1b.")
proxy.initializeValues(90,50)
print(proxy.scenario1())
runtests = input("Press enter to start Scenario 1b.")
proxy.initializeValues(90,50)
print(proxy.scenario2())
runtests = input("Press enter to start Scenario 1ci.")
proxy.initializeValues(200,300)
print(proxy.scenario1c(True))
runtests = input("Press enter to start Scenario 2ci.")
proxy.initializeValues(200,300)
print(proxy.scenario2c(True))
runtests = input("Press enter to start Scenario 1cii.")
proxy.initializeValues(200,300)
print(proxy.scenario1c(False))
runtests = input("Press enter to start Scenario 2cii.")
proxy.initializeValues(200,300)
print(proxy.scenario2c(False))