"""
DAQ-like showcase of ZeroMQ

Detectors produce data, load balancer distributes it to workers who do event reconstruction, data is then collected by the sink.

Author: Krzysztof Furman <k(dot)furman(at)qmul(dot)ac(dot)uk>
"""

from __future__ import print_function

import multiprocessing
import zmq
import time
import random

# Set amount of detectors and event builders
N_DETECTORS = 4
N_WORKERS = 5


def detector_task(ident):
    # Using PUSH socket to send detector readout
    socket = zmq.Context().socket(zmq.PUSH)
    socket.identity = u"Detector-{}".format(ident).encode("ascii")
    socket.connect("tcp://127.0.0.1:5557")

    # Send 10 readouts
    for readout in range(10): #change to while True for continuous production
        print("Sending data from Detector-{}".format(ident))
        time.sleep(0.5) #delay between readouts
        event = random.randint(0,1) #produce random data
        if event == 0:
            socket.send(b"MISS")
        if event == 1:
            socket.send(b"PARTICLE")


def worker_task(ident):
    # Using PULL socket to receive detector data
    socket = zmq.Context().socket(zmq.PULL)
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    socket.connect("tcp://127.0.0.1:5558")
    # Using PUSH socket to send to a sink
    sender = zmq.Context().socket(zmq.PUSH)
    sender.connect("tcp://127.0.0.1:5559")

    # Receive data, and send to sink if particle
    while True:
        data = socket.recv()
        print("Processing: {} - {}".format(socket.identity.decode("ascii"), data.decode("ascii")))
        time.sleep(1) #delay to simulate event building time
        if data.decode("ascii") == "PARTICLE":
            sender.send(b'1')
        print("(SUCCESS) Processed: {} - {}".format(socket.identity.decode("ascii"), data.decode("ascii")))
        

def collector_task():
    # Using PULL socket to collect data
    sink = zmq.Context().socket(zmq.PULL)
    sink.bind("tcp://127.0.0.1:5559")
    hits=0
    # Collect hits
    while True:
        hits += int(sink.recv())
        print("Total particles detected: ", hits)


def main():
    # Mail loop of the load balancer
    # Context and sockets
    context = zmq.Context.instance()
    frontend = context.socket(zmq.PULL)
    frontend.bind("tcp://127.0.0.1:5557")
    backend = context.socket(zmq.PUSH)
    backend.bind("tcp://127.0.0.1:5558")
    
    # Start background tasks
    def start(task, *args):
        process = multiprocessing.Process(target=task, args=args)
        process.daemon = True
        process.start()
    for i in range(N_DETECTORS):
        start(detector_task, i)
    for i in range(N_WORKERS):
        start(worker_task, i)
    start(collector_task)
    
    # Use zmq.proxy() to handle load balancing
    while True:
        zmq.proxy(frontend,backend) #very powerful and convenient if no specific ending conditions needed, otherhwise should do it manually with zmq.POLLIN etc.

    # Clean up
    backend.close()
    frontend.close()
    sink.close()
    context.term()


if __name__ == "__main__":
    main()