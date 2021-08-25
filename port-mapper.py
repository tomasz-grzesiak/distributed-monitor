#!/usr/bin/python3

import zmq
import resources.message_pb2 as protobuf

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

# while True:
request = protobuf.InitRequestMessage()
request.ParseFromString(socket.recv())
print("Received request: %s" % request)
response = protobuf.InitResponseMessage()
response.processID = 1;
response.portMapperAddress = '111.222.111.222'
response.addresses.append('1.2.3.4')
response.addresses.append('2.3.4.5')
print("Sending response: %s" % response)
socket.send(response.SerializeToString())
