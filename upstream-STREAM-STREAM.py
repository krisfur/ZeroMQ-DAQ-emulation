"""
Upstream DAQ-like showcase of ZeroMQ

Detectors produce data and send it on raw TCP STREAM sockets, data is then collected by the sink.

Author: Krzysztof Furman <k(dot)furman(at)qmul(dot)ac(dot)uk>
"""

from __future__ import print_function

import multiprocessing
import zmq
import time
import random

# Set amount of detectors and event builders
N_DETECTORS = 4


def detector_task(ident):
    # Using STREAM socket to send detector readout
    socket = zmq.Context().socket(zmq.STREAM)
    socket.identity = u"Detector-{}".format(ident).encode("ascii")
    socket.connect("tcp://127.0.0.1:5557")

    # Send 10 readouts
    for readout in range(10): #change to while True for continuous production
        print("Sending data from Detector-{}".format(ident))
        time.sleep(0.5) #delay between readouts
        event = random.randint(0,1) #produce random data
        if event == 0:
            socket.send_multipart([socket.identity,b"MISS"])
        if event == 1:
            socket.send_multipart([socket.identity,b"PARTICLE"])
    socket.close()
    quit()


def main():
    
    # Start background tasks
    def start(task, *args):
        process = multiprocessing.Process(target=task, args=args)
        process.daemon = True
        process.start()
    for i in range(N_DETECTORS):
        start(detector_task, i)

    sink = zmq.Context().socket(zmq.STREAM)
    sink.bind("tcp://127.0.0.1:5557")
    hits=0
    # Collect hits
    while True:
        data = sink.recv_multipart()
        if len(data) > 1:
            print(data[1])
            if data[1].decode("ascii") == "PARTICLE":
                hits += 1
        print("Total particles detected: ", hits)



if __name__ == "__main__":
    main()