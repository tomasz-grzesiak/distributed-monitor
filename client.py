#!/usr/bin/python3

import zmq
import protobuf
from threading import Thread

own_socket = None
port_mapper_socket = None
port_mapper_sub_socket = None
node_sockets = []


def set_up_port_mapper_zmq(context):
    global own_socket
    own_socket = context.socket(zmq.PUB)
    own_port = own_socket.bind_to_random_port('tcp://*')    # own socket for communication with other processes

    global port_mapper_socket
    port_mapper_socket = context.socket(zmq.REQ)
    port_mapper_socket.connect('tcp://localhost:5555')  # connection with portMapper

    return own_port


def set_up_node_zmq(address, context):
    socket = context.socket(zmq.SUB)
    socket.connect(address)
    return socket


def build_init_request(port: int, ready: bool):
    request = protobuf.InitRequestMessage()
    request.address = f'tcp://localhost:{port}'
    request.ready = ready
    return request


def init():
    context = zmq.Context()
    own_port = set_up_port_mapper_zmq(context)
    request = build_init_request(own_port, False)
    print(request)
    port_mapper_socket.send(request.SerializeToString())

    response = protobuf.InitResponseMessage()
    response.ParseFromString(port_mapper_socket.recv())
    print(response)

    global node_sockets
    node_sockets = [set_up_node_zmq(address, context) for address in response.addresses if str(own_port) not in address]
    global port_mapper_sub_socket
    port_mapper_sub_socket = context.socket(zmq.SUB)
    port_mapper_sub_socket.connect(f'tcp://localhost:{str(response.portMapperAddress).split(":")[2]}')

    request = build_init_request(own_port, True)
    port_mapper_socket.send(request.SerializeToString())
    port_mapper_socket.close()


def init_zmq():
    if own_socket is None:
        init()


def listen():
    pass


def wait():
    init_zmq()
    first_thread = Thread(target=listen)
    first_thread.start()


if __name__ == '__main__':
    wait()
