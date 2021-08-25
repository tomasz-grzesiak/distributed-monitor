#!/usr/bin/python3

import zmq
import resources.message_pb2 as protobuf

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")

request = protobuf.InitRequestMessage()
request.address = "tcp://localhost:5555"
request.ready = True
print(request)
socket.send(request.SerializeToString())

response = protobuf.InitResponseMessage()
response.ParseFromString(socket.recv())
print(response)