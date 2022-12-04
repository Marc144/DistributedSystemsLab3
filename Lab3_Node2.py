import pickle
from multiprocessing.connection import Listener, Client
from threading import Thread
max_id = 0
server = "A"
Avalue = 0
Bvalue = 0

class RPCHandler:
    def __init__(self):
        self._functions = { }

    def register_function(self, func):
        self._functions[func.__name__] = func

    def handle_connection(self, connection):
        try:
            while True:
                # Receive a message
                func_name, args, kwargs = pickle.loads(connection.recv())
                # Run the RPC and send a response
                try:
                    r = self._functions[func_name](*args,**kwargs)
                    connection.send(pickle.dumps(r))
                except Exception as e:
                    connection.send(pickle.dumps(e))
        except EOFError:
             pass

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



def rpc_server(handler, address, authkey):
    sock = Listener(address, authkey=authkey)
    while True:
        client = sock.accept()
        t = Thread(target=handler.handle_connection, args=(client,))
        t.daemon = True
        t.start()

# Some remote functions
def getAmount(server):
    readAmountFromFile()
    if server == "A":
        return Avalue
    if server == "B":
        return Bvalue
    return 0

def readAmountFromFile():
    global Avalue,Bvalue
    try:
        with open('accounts.txt', 'r') as f:
            serv = f.readline()
            if serv == "A":
                Avalue = int(f.readline())
            serv = f.readline()
            if serv == "B":
                Avalue = int(f.readline())
            f.close()
    except FileNotFoundError:
        print("no such directory")
    print(Avalue)
    print(Bvalue)


def commitChanges(serv):
    global server
    if serv == server:
        try:
            with open('accounts.txt' , 'w') as f:
                f.write("A")
                f.write("\n")
                f.write(str(Avalue))
                f.write("\n")
                f.write("B")
                f.write("\n")
                f.write(str(Bvalue))
                f.close()
        except FileNotFoundError:
            print("no such directory")
            return False
    return True


def requestDeposit(amount, serv):
    readAmountFromFile()
    return True
    
def requestFunds(amount, serv):
    readAmountFromFile()
    global Avalue, Bvalue
    requested_value = 0
    if serv == "A":
        requested_value = Avalue
    if serv == "B":
        requested_value = Bvalue
    if requested_value > amount:
        return True
    return False

def commitAddFundsTransaction(amount, serv):
    global server, Avalue, Bvalue
    if serv == server:
        if server == "A":
            Avalue += amount
        if server == "B":
            Bvalue += amount
    output = commitChanges(server)
    return output

def closeServer():
    exit()

# Register with a handler
handler = RPCHandler()
handler.register_function(getAmount)
handler.register_function(commitAddFundsTransaction)
handler.register_function(requestFunds)
handler.register_function(requestDeposit)
handler.register_function(closeServer)

# Run the server
print("Server "+ server +" started")

rpc_server(handler, ('localhost', 17001), authkey=b'peekaboo')