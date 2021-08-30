#!/usr/bin/python3

import zmq
import resources.message_pb2 as protobuf


def set_up_port_mapper_zmq(context):
    own_socket = context.socket(zmq.PUB)
    own_port = own_socket.bind_to_random_port('tcp://*')    # own socket for communication with other processes

    port_mapper_socket = context.socket(zmq.REQ)
    port_mapper_socket.connect('tcp://localhost:5555')  # connection with portMapper

    return [port_mapper_socket, own_socket, own_port]


def set_up_node_zmq(address, context):
    print(address)
    socket = context.socket(zmq.SUB)
    socket.connect(address)
    return socket


def build_init_request(port: int, ready: bool):
    request = protobuf.InitRequestMessage()
    request.address = f'tcp://localhost:{port}'
    request.ready = ready
    return request


def main():
    context = zmq.Context()
    [port_mapper_socket, own_socket, own_port] = set_up_port_mapper_zmq(context)
    request = build_init_request(own_port, False)
    print(request)
    port_mapper_socket.send(request.SerializeToString())

    response = protobuf.InitResponseMessage()
    response.ParseFromString(port_mapper_socket.recv())
    print(response)

    node_sockets = [set_up_node_zmq(address, context) for address in response.addresses if str(own_port) not in address]

    request = build_init_request(own_port, True)
    print(request)
    port_mapper_socket.send(request.SerializeToString())


if __name__ == '__main__':
    main()
