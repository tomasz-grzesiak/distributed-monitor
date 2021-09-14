import zmq
import protobuf
from threading import Thread, Lock
from .shared_object import SharedObject, SharedObjectState, VectorClockComparisonResult


class SharedObjectContext:
    def __init__(self):
        self.context = zmq.Context()

        # before first message - create own zmq socket, establish req-rep connection with portMapper
        self.own_socket = self.context.socket(zmq.PUB)
        self.own_port = self.own_socket.bind_to_random_port('tcp://*')
        self.port_mapper_req_socket = self.context.socket(zmq.REQ)
        self.port_mapper_req_socket.connect('tcp://localhost:5555')

        # exchange first initialization message with portMapper
        request = self._build_init_request(False)
        self.port_mapper_req_socket.send(request.SerializeToString())
        response = protobuf.InitResponseMessage()
        response.ParseFromString(self.port_mapper_req_socket.recv())

        # after first message - establish pub-sub connection with portMapper and other nodes in cluster
        self.processID = response.processID
        self._set_up_sockets(response)
        self.clock = [0] * (len(self.node_sockets) + 1)
        self.managed_objects = {}

        # exchange second initialization message with portMapper and close req-rep socket
        request = self._build_init_request(True)
        self.port_mapper_req_socket.send(request.SerializeToString())
        self.port_mapper_req_socket.recv()
        self.port_mapper_req_socket.close()

        # start threads listening to messages and create lock to guard state
        self.processes_lock = Lock()
        self.clock_lock = Lock()
        Thread(target=self._listen_to_new_node).start()
        with self.processes_lock:
            self.new_connection_thread = None
            if len(self.node_sockets) > 0:
                self.new_connection_thread = Thread(target=self._listen_to_synchro_messages)
                self.new_connection_thread.start()

    def __str__(self):
        return f'ConnectionManager(processId={self.processID}, socket_port={self.own_port}, known_nodes_counter={len(self.node_sockets)}'

    def __del__(self):
        self.own_socket.close()
        self.port_mapper_sub_socket.close()
        for socket in self.node_sockets:
            socket.close()
        self.context.destroy()

    def _set_up_sockets(self, init_response: protobuf.InitResponseMessage):
        # establish pub-sub connection with portMapper
        self.port_mapper_sub_socket = zmq.Context().socket(zmq.SUB)
        self.port_mapper_sub_socket.connect(f'tcp://localhost:{str(init_response.portMapperAddress).split(":")[-1]}')
        self.port_mapper_sub_socket.subscribe('')

        # establish pub-sub connections with other nodes in cluster
        self.node_sockets = [self._set_up_node_zmq(address) for address in init_response.addresses
                             if str(self.own_port) not in address]

    def _set_up_node_zmq(self, address: str):
        socket = self.context.socket(zmq.SUB)
        socket.connect(address)
        socket.subscribe('')
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
                with self.processes_lock:
                    self.node_sockets.append(self._set_up_node_zmq(new_connection_message.address))
                    with self.clock_lock:
                        self.clock.append(0)
                    if self.new_connection_thread is None:
                        self.new_connection_thread = Thread(target=self._listen_to_synchro_messages)
                        self.new_connection_thread.start()
                    print('New node joined cluster, address=%s' % new_connection_message.address)

    def _listen_to_synchro_messages(self):
        poller = zmq.Poller()
        while True:
            # główna pętla odbierania wiadomości
            with self.processes_lock:
                for node_socket in self.node_sockets:
                    poller.register(node_socket, zmq.POLLIN)
                    poller.register(node_socket, zmq.POLLIN)
            sockets_with_message = dict(poller.poll())
            for socket_with_message in sockets_with_message:
                self._handle_synchro_message(socket_with_message)

    def _handle_synchro_message(self, socket: zmq.Socket):
        synchro_message = protobuf.SynchroMessage()
        synchro_message.ParseFromString(socket.recv())
        if len(synchro_message.receiverProcessID) > 0 and self.processID not in synchro_message.receiverProcessID:
            return

        if synchro_message.objectID not in self.managed_objects:
            self._handle_message_for_unknown_object(synchro_message)
        else:
            self._handle_message_for_known_object(synchro_message)

    def _handle_message_for_unknown_object(self, synchro_message: protobuf.SynchroMessage):
        pass

    def _handle_message_for_known_object(self, synchro_message: protobuf.SynchroMessage):
        print('received message', synchro_message)
        message_type = synchro_message.type
        message_clock = list(synchro_message.clock)
        self._synchronize_clock(message_clock)
        message_object: SharedObject = self.managed_objects[synchro_message.objectID]
        if message_type == protobuf.SynchroMessage.LOCK_REQ:
            with message_object.condition_lock:
                state = message_object.state
                if state == SharedObjectState.INACTIVE:
                    self._send_lock_ack_message(message_object.name, [synchro_message.processID])
                    print('Odpowiadam LOCK_ACK dla procesu ', synchro_message.processID)
                if state == SharedObjectState.WAITING_FOR_LOCK_ACK:
                    comparison_result = message_object.compare_clock(message_clock)
                    if comparison_result == VectorClockComparisonResult.GREATER or (comparison_result == VectorClockComparisonResult.NON_COMPARABLE and synchro_message.processID < self.processID):
                        self._send_lock_ack_message(message_object.name, [synchro_message.processID])
                        print('Odpowiadam LOCK_ACK dla procesu ', synchro_message.processID)
                    if comparison_result == VectorClockComparisonResult.SMALLER or (comparison_result == VectorClockComparisonResult.NON_COMPARABLE and synchro_message.processID > self.processID):
                        message_object.waiting_for_lock_ack.append(synchro_message.processID)
                        print('Wstrzymuję LOCK_ACK dla procesu ', synchro_message.processID)
                if state == SharedObjectState.LOCKED:
                    message_object.waiting_for_lock_ack.append(synchro_message.processID)
                    print('Wstrzymuję LOCK_ACK dla procesu ', synchro_message.processID)
        if message_type == protobuf.SynchroMessage.LOCK_ACK:
            with message_object.condition_lock:
                message_object.remaining_lock_ack_counter -= 1  # nie potrzeba zabezpieczenia na ujemne?
                if message_object.remaining_lock_ack_counter == 0:
                    message_object.condition_lock.notify()
                print('Odbieram LOCK_ACK, wartość licznika ', message_object.remaining_lock_ack_counter)
        # TODO obsługa wiadomości kolejnych typów

    def _synchronize_clock(self, message_clock: list):
        with self.clock_lock:
            min_length = min(len(self.clock), len(message_clock))
            for i in range(min_length):
                self.clock[i] = max(self.clock[i], message_clock[i])
            if len(self.clock) < len(message_clock):
                self.clock.extend(message_clock[min_length:])
            self.clock[self.processID - 1] += 1

    def shared_object(self, object_id: str):
        shared_object = SharedObject(object_id, self)
        self.managed_objects[object_id] = shared_object
        return shared_object

    def perform_lock(self, object_id: str):
        message = self._build_synchro_message(object_id, protobuf.SynchroMessage.LOCK_REQ)
        self._send_synchro_message(message)
        print('Send LOCK_REQ message=%s' % message)
        with self.processes_lock:
            self.managed_objects[object_id].remaining_lock_ack_counter = len(self.node_sockets)

    def perform_unlock(self, object_id: str):
        shared_object: SharedObject = self.managed_objects[object_id]
        if len(shared_object.waiting_for_lock_ack) > 0:
            self._send_lock_ack_message(object_id, shared_object.waiting_for_lock_ack)

    def _build_synchro_message(self, object_id: str,
                               message_type: protobuf.SynchroMessage,
                               receiver_ids: list = []) -> protobuf.SynchroMessage:
        synchro_message = protobuf.SynchroMessage()
        synchro_message.processID = self.processID
        synchro_message.objectID = object_id
        synchro_message.type = message_type
        for receiver_id in receiver_ids:
            synchro_message.receiverProcessID.append(receiver_id)
        return synchro_message

    def _send_lock_ack_message(self, object_id: str, receiver_ids: list):
        self._send_synchro_message(self._build_synchro_message(object_id, protobuf.SynchroMessage.LOCK_ACK, receiver_ids))

    def _send_synchro_message(self, synchro_message: protobuf.SynchroMessage):
        with self.clock_lock:
            self.clock[self.processID - 1] += 1
            for single_process_clock in self.clock:
                synchro_message.clock.append(single_process_clock)
        self.own_socket.send(synchro_message.SerializeToString())
