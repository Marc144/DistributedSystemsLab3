# -*- coding:utf-8 -*-

import pickle
from multiprocessing.connection import Listener, Client
from threading import Thread
max_id = 0
server = "Coordinator"
serverA = "A"
serverB = "B"
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

def closeServers():
    connA = Client(('localhost', 17001), authkey=b'peekaboo')
    connB = Client(('localhost', 17002), authkey=b'peekaboo')
    proxyA = RPCProxy(connA)
    proxyB = RPCProxy(connB)
    proxyA.closeServer()
    proxyB.closeServer()
    exit()

def initializeValues(a, b):
    try:
        with open('accounts.txt' , 'w') as f:
            f.write("A")
            f.write("\n")
            f.write(str(a))
            f.write("\n")
            f.write("B")
            f.write("\n")
            f.write(str(b))
            f.close()
    except FileNotFoundError:
        print("no such directory")

#regular scenario 1
def scenario1():
    global server, serverA, serverB
    connA = Client(('localhost', 17001), authkey=b'peekaboo')
    connB = Client(('localhost', 17002), authkey=b'peekaboo')
    proxyA = RPCProxy(connA)
    proxyB = RPCProxy(connB)
    proxies = [proxyA,proxyB]
    servers = [serverA, serverB]
    scenario1description = "Scenario 1: Transaction 1 occurs before Transaction 2 "
    abortedResponse = scenario1description + "Failed."
    if transaction1(proxies,servers) == False:
        return abortedResponse
    if transaction2(proxies,servers) == False:
        return abortedResponse
    successMessage = scenario1description + "Succeeded."
    return successMessage

#regular scenario 2
def scenario2():
    global server, serverA, serverB
    connA = Client(('localhost', 17001), authkey=b'peekaboo')
    connB = Client(('localhost', 17002), authkey=b'peekaboo')
    proxyA = RPCProxy(connA)
    proxyB = RPCProxy(connB)
    proxies = [proxyA,proxyB]
    servers = [serverA, serverB]
    scenario1description = "Scenario 2: Transaction 2 occurs before Transaction 1 "
    abortedResponse = scenario1description + "Failed."
    if transaction2(proxies,servers) == False:
        return abortedResponse
    if transaction1(proxies,servers) == False:
        return abortedResponse
    successMessage = scenario1description + "Succeeded."
    return successMessage


#scenario 1 with crash
def scenario1C(crash):
    global server, serverA, serverB
    connA = Client(('localhost', 17001), authkey=b'peekaboo')
    connB = Client(('localhost', 17002), authkey=b'peekaboo')
    proxyA = RPCProxy(connA)
    proxyB = RPCProxy(connB)
    proxies = [proxyA,proxyB]
    servers = [serverA, serverB]
    scenario1description = "Scenario 1: Transaction 1 occurs before Transaction 2 "
    abortedResponse = scenario1description + "Failed."
    if transaction1C(proxies,servers, crash) == False:
        return abortedResponse
    if transaction2C(proxies,servers, crash) == False:
        return abortedResponse
    successMessage = scenario1description + "Succeeded."
    return successMessage

#scenario 2 with crash
def scenario2C(crash):
    global server, serverA, serverB
    connA = Client(('localhost', 17001), authkey=b'peekaboo')
    connB = Client(('localhost', 17002), authkey=b'peekaboo')
    proxyA = RPCProxy(connA)
    proxyB = RPCProxy(connB)
    proxies = [proxyA,proxyB]
    servers = [serverA, serverB]
    scenario1description = "Scenario 2: Transaction 2 occurs before Transaction 1 "
    abortedResponse = scenario1description + "Failed."
    if transaction2C(proxies,servers, crash) == False:
        return abortedResponse
    if transaction1C(proxies,servers, crash) == False:
        return abortedResponse
    successMessage = scenario1description + "Succeeded."
    return successMessage


#regular transaction 1
def transaction1(proxies):
    global server, serverA, serverB
    responses = []
    for i in range(len(proxies)):
        responses.append(sendFundRequest(proxies[i], 100, serverA))
    status = True
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    for i in range(len(proxies)):
        responses.append(sendCommitAddFunds(proxies[i], -100, serverA))
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome

    for i in range(len(proxies)):
        responses.append(fundDepositRequest(proxies[i], 100, serverB))
    status = True
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    for i in range(len(proxies)):
        responses.append(sendCommitAddFunds(proxies[i], 100, serverB))
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    return True
    
#regular transaction 2
def transaction2(proxies):
    global server, serverA, serverB
    responses = []
    accountABalance = fundAmountRequest(proxies[0], serverA)
    valueToAdd = accountABalance * .2
    #add to A
    for i in range(len(proxies)):
        responses.append(fundDepositRequest(proxies[i], valueToAdd, serverA))
    status = True
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    for i in range(len(proxies)):
        responses.append(sendCommitAddFunds(proxies[i], valueToAdd, serverA))
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    #add to B
    for i in range(len(proxies)):
        responses.append(fundDepositRequest(proxies[i], valueToAdd, serverB))
    status = True
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    for i in range(len(proxies)):
        responses.append(sendCommitAddFunds(proxies[i], valueToAdd, serverB))
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    return True




#transaction 1 with crash
def transaction1C(proxies, crash):
    global server, serverA, serverB
    responses = []
    for i in range(len(proxies)):
        responses.append(sendFundRequestwithCrash(proxies[i], 100, serverA, crash))
    status = True
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    for i in range(len(proxies)):
        responses.append(sendCommitAddFundswithCrash(proxies[i], -100, serverA, crash))
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome

    for i in range(len(proxies)):
        responses.append(fundDepositRequestwithCrash(proxies[i], 100, serverB, crash))
    status = True
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    for i in range(len(proxies)):
        responses.append(sendCommitAddFundswithCrash(proxies[i], 100, serverB, crash))
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    return True
    
#transaction 2 with crash
def transaction2C(proxies, crash):
    global server, serverA, serverB
    responses = []
    accountABalance = fundAmountRequestwithCrash(proxies[0], serverA, crash)
    valueToAdd = accountABalance * .2
    #add to A
    for i in range(len(proxies)):
        responses.append(fundDepositRequestwithCrash(proxies[i], valueToAdd, serverA, crash))
    status = True
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    for i in range(len(proxies)):
        responses.append(sendCommitAddFundswithCrash(proxies[i], valueToAdd, serverA, crash))
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    #add to B
    for i in range(len(proxies)):
        responses.append(fundDepositRequestwithCrash(proxies[i], valueToAdd, serverB, crash))
    status = True
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    for i in range(len(proxies)):
        responses.append(sendCommitAddFunds(proxies[i], valueToAdd, serverB))
    for i in responses:
        if i == False:
            status = False
    if status == False:
        outcome = False
        return outcome
    return True
    
#RPC calls to Nodes
def fundAmountRequest(proxy, server):
    amount = 0
    amount = proxy.getAmount(server)
    return amount

def fundDepositRequest(proxy, amount, server):
    output = False
    output = proxy.requestDeposit(amount, server)
    return output

def sendFundRequest(proxy, amount, server):
    output = False
    output = proxy.requestFunds(amount, server)
    return output

def sendCommitAddFunds(proxy, amount, server):
    output = False
    output = proxy.commitAddFundsTransaction(amount,server)
    return output


#RPC calls to Nodes with crash simulation
def fundAmountRequestwithCrash(proxy, server, crashtime):
    amount = 0
    amount = proxy.getAmountwithCrash(server, crashtime)
    return amount

def fundDepositRequestwithCrash(proxy, amount, server, crashtime):
    output = False
    output = proxy.requestDepositwithCrash(amount, server, crashtime)
    return output

def sendFundRequestwithCrash(proxy, amount, server, crashtime):
    output = False
    output = proxy.requestFundswithCrash(amount, server, crashtime)
    return output

def sendCommitAddFundswithCrash(proxy, amount, server, crashtime):
    output = False
    output = proxy.commitAddFundsTransactionwithCrash(amount,server, crashtime)
    return output


# Register with a handler
handler = RPCHandler()
handler.register_function(scenario1)
handler.register_function(scenario2)
handler.register_function(scenario1C)
handler.register_function(scenario2C)
handler.register_function(initializeValues)
handler.register_function(closeServers)

# Run the server
print("Server "+ server +" started")
try:
    with open('accounts.txt' , 'w') as f:
        f.close()
except FileNotFoundError:
    print("no such directory")
rpc_server(handler, ('localhost', 17000), authkey=b'peekaboo')