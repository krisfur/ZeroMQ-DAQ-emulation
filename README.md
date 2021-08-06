# ZeroMQ-DAQ-Emulation
Emulating a primitive multithreaded DAQ setup with fake detectors and event builders.

Requirements:
- pyzmq (tested on version 18.1.1)

## upstream-STREAM-STREAM.py:
- Simple STREAM-STREAM network with no specialised redundancy beyond default offerings from ZeroMQ
- Scalable amount of detectors send "PARTICLE" or "MISS" via STREAM socket (raw TCP)
- Sink collects on a STREAM socket and totals "PARTICLE" hits

## DAQ-push-pull.py:
- Simple PUSH-PULL network with no specialised redundancy beyond default offerings from ZeroMQ
- Scalable amount of detectors feed into a load balancer, which distributes the work to scalable amount of workers
- Workers perform event building and parse "hits" to a sink collector.

![PUSH-PULL schematic](graphics/DAQ-push-pull.jpg)
