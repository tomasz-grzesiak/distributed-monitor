import zmq
import protobuf
from threading import Thread
import time


class NewConnectionThread(Thread):
    def __init__(self):
        Thread.__init__(self)

    def terminate(self):
        self._stop()


class ConnectionManager:
    def __init__(self):
        self.context = zmq.Context()

        self.own_socket = self.context.socket(zmq.PUB)  # own socket for publishing data
        self.own_port = self.own_socket.bind_to_random_port('tcp://*')

        self.port_mapper_req_socket = self.context.socket(zmq.REQ)
        self.port_mapper_req_socket.connect('tcp://localhost:5555')  # connection with portMapper during initialization
        request = self._build_init_request(False)
        self.port_mapper_req_socket.send(request.SerializeToString())

        response = protobuf.InitResponseMessage()
        response.ParseFromString(self.port_mapper_req_socket.recv())

        self.processID = response.processID
        self._set_up_sockets(response)

        request = self._build_init_request(True)
        self.port_mapper_req_socket.send(request.SerializeToString())
        self.port_mapper_req_socket.recv()
        self.port_mapper_req_socket.close()

    def __str__(self):
        return f'ConnectionManager(processId={self.processID}, socket_port={self.own_port}, known_nodes_counter={len(self.node_sockets)}'

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.new_connection_process.terminate()
        self.own_socket.close()
        self.port_mapper_sub_socket.close()
        for socket in self.node_sockets:
            socket.close()
        self.context.destroy()

    def _set_up_sockets(self, init_response):
        self.port_mapper_sub_socket = self.context.socket(zmq.SUB)  # receives message on new connection
        self.port_mapper_sub_socket.setsockopt_string(zmq.SUBSCRIBE, '')
        self.port_mapper_sub_socket.connect(f'tcp://localhost:{str(init_response.portMapperAddress).split(":")[-1]}')
        print('before process invocation')
        self.new_connection_process = multiprocessing.Process(target=self.listen_to_new_node())
        print('before process')
        self.new_connection_process.start()
        print('after process')

        self.node_sockets = [self._set_up_node_zmq(address) for address in init_response.addresses
                             if str(self.own_port) not in address]  # receives messages from other nodes

    def _set_up_node_zmq(self, address):
        socket = self.context.socket(zmq.SUB)
        socket.connect(address)
        return socket

    def _build_init_request(self, ready):
        request = protobuf.InitRequestMessage()
        request.address = f'tcp://localhost:{self.own_port}'
        request.ready = ready
        return request

    def listen_to_new_node(self):
        # while True:
        print('listening to new connection message...')
        time.sleep(7)
        new_connection_message = protobuf.NewConnectionMessage()
        # new_connection_message.ParseFromString(self.port_mapper_sub_socket.recv())
        print(new_connection_message)
        # self.node_sockets.append(self._set_up_node_zmq(new_connection_message.address))
