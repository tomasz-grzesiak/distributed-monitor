#!/usr/bin/python3

import zmq
import protobuf


processes = {}
max_process_number = 0
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


def build_init_response():
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
    while True:
        request = read_init_request()
        print('Received request: %s' % request)
        global max_process_number
        max_process_number = max_process_number + 1
        processes[max_process_number] = request.address

        response = build_init_response()
        print('Sending response: %s' % response)
        rep_socket.send(response.SerializeToString())

        ready_request = read_init_request()
        print('Received request: %s' % ready_request)
        rep_socket.send_string('')
        if ready_request.address != request.address:
            print('Source of messages does not match, request=%s, ready_request=%s'
                  % (request.address, ready_request.address))
            processes.pop(max_process_number)
            continue

        send_new_connection_message(ready_request.address)


if __name__ == '__main__':
    main()
