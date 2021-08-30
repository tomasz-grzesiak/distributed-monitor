#!/usr/bin/python3

import zmq
import resources.message_pb2 as protobuf


processes = {}
max_process_number = 0
pub_address = 'tcp://*:5566'


def set_up_zmq():
    context = zmq.Context()
    rep_socket = context.socket(zmq.REP)
    rep_socket.bind("tcp://*:5555")

    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind(pub_address)
    return [rep_socket, pub_socket]


def read_init_request(socket):
    request = protobuf.InitRequestMessage()
    request.ParseFromString(socket.recv())
    return request


def build_init_response():
    response = protobuf.InitResponseMessage()
    response.processID = max_process_number
    response.portMapperAddress = pub_address
    for node_address in processes.values():
        response.addresses.append(node_address)
    return response


def main():
    [rep_socket, pub_socket] = set_up_zmq()
    while True:
        request = read_init_request(rep_socket)
        print('Received request: %s' % request)
        global max_process_number
        max_process_number = max_process_number + 1
        processes[max_process_number] = request.address

        response = build_init_response()
        print('Sending response: %s' % response)
        rep_socket.send(response.SerializeToString())

        ready_request = read_init_request(rep_socket)
        if ready_request.address != request.address:
            print('Source of messages does not match, request=%s, ready_request=%s'
                  % request.address, ready_request.address)
            return


if __name__ == '__main__':
    main()
