import zmq
import protobuf
from threading import Thread
from copy import deepcopy


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
        message = self.port_mapper_req_socket.recv()
        response.ParseFromString(message)

        self.processID = response.processID
        self._set_up_sockets(response)
        self.clock = [0] * len(self.node_sockets) + 1

        request = self._build_init_request(True)
        self.port_mapper_req_socket.send(request.SerializeToString())
        self.port_mapper_req_socket.recv()
        self.port_mapper_req_socket.close()

        Thread(target=self._listen_to_new_node).start()

    def __str__(self):
        return f'ConnectionManager(processId={self.processID}, socket_port={self.own_port}, known_nodes_counter={len(self.node_sockets)}'

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.own_socket.close()
        self.port_mapper_sub_socket.close()
        for socket in self.node_sockets:
            socket.close()
        self.context.destroy()

    def _set_up_sockets(self, init_response):
        self.port_mapper_sub_socket = zmq.Context().socket(zmq.SUB)  # receives message on new connection
        self.port_mapper_sub_socket.connect(f'tcp://localhost:{str(init_response.portMapperAddress).split(":")[-1]}')
        self.port_mapper_sub_socket.subscribe('')

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

    def _listen_to_new_node(self):
        while True:
            new_connection_message = protobuf.NewConnectionMessage()
            new_connection_message.ParseFromString(self.port_mapper_sub_socket.recv())
            if str(new_connection_message.address).split(':')[-1] != str(self.own_port):
                self.node_sockets.append(self._set_up_node_zmq(new_connection_message.address))
                print('New node joined cluster, address=%s' % new_connection_message.address)

    def perform_lock(self, object_id):
        self._send_lock_req(object_id)
        nodes_for_ack = deepcopy(self.node_sockets)
        poller = zmq.Poller()
        while len(nodes_for_ack) > 0:
            for node in nodes_for_ack:
                poller.register(node, zmq.POLLIN)
            socks = dict(poller.poll())
            for sock in socks.values():
                message = protobuf.SynchroMessage()
                message.ParseFromString(sock.recv())
                if message.type == protobuf.SynchroMessage.UNLOCK:
                    nodes_for_ack.remove(sock)

    def _send_lock_req(self, object_id):
        lock_req_message = protobuf.SynchroMessage()
        lock_req_message.processID = self.processID
        lock_req_message.objectID = object_id
        lock_req_message.type = protobuf.SynchroMessage.LOCK
        self.own_socket.send(lock_req_message.SerializeToString())
        print('Send LOCK message=%s' % lock_req_message)
