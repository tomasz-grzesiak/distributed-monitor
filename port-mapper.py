#!/usr/bin/python3

import zmq
import protobuf


processes = {}
rep_address = 'tcp://*:5555'
pub_address = 'tcp://*:5566'
rep_socket = None
pub_socket = None


def set_up_zmq():
    context = zmq.Context()
    global rep_socket
    rep_socket = context.socket(zmq.REP)
    rep_socket.bind(rep_address)

    global pub_socket
    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind(pub_address)


def read_init_request():
    request = protobuf.InitRequestMessage()
    request.ParseFromString(rep_socket.recv())
    return request


def build_init_response(max_process_number: int):
    response = protobuf.InitResponseMessage()
    response.processID = max_process_number
    response.portMapperAddress = pub_address
    for node_address in processes.values():
        response.addresses.append(node_address)
    return response


def send_new_connection_message(address):
    message = protobuf.NewConnectionMessage()
    message.address = address
    pub_socket.send(message.SerializeToString())


def main():
    set_up_zmq()
    max_process_number = 0
    while True:
        request = read_init_request()
        if request.ready:
            rep_socket.send(b'')
            print('New node successfully connected, address=%s, broadcasting to other nodes...' % request.address)
            processes[max_process_number] = request.address
            send_new_connection_message(request.address)
            print('Active nodes in cluster: %s' % processes)
        else:
            print('New node joining cluster, address=%s' % request.address)
            max_process_number = max_process_number + 1
            response = build_init_response(max_process_number)
            rep_socket.send(response.SerializeToString())


if __name__ == '__main__':
    main()
